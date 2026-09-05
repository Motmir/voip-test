use std::{collections::HashMap, env, fs::File, io::{BufReader, BufWriter, Result, Write, stdin, stdout}, net::{IpAddr, SocketAddr, UdpSocket}, str::FromStr, sync::{Arc, Mutex}, thread, time::Duration};
use cpal::{StreamConfig, traits::{DeviceTrait, HostTrait, StreamTrait}};
use ringbuf::{HeapRb, traits::*};
use opus::{Application, Channels, Encoder, Decoder};
use serde::{Deserialize, Serialize};
use rsip::{Method, Request, SipMessage, Uri, Version, headers::{CSeq, CallId, From, MaxForwards, To, Via}};
use rsip::prelude::*;


const DEFAULT_PORT: u16 = 9999;
const MAX_PACKET_PAYLOAD_SIZE: usize = 960;
const SEQ_SIZE: usize = 2;
const TIMESTAMP_SIZE: usize = 4;
const PACKET_TOTAL_SIZE: usize =  SEQ_SIZE + TIMESTAMP_SIZE + MAX_PACKET_PAYLOAD_SIZE;
const SAMPLE_RATE: usize = 48000;
const CHANNELS_COUNT: usize = 2;
const BYTES_PER_SAMPLE: usize = 2;

struct Packet {
    seq: u16,
    timestamp: u32,
    payload_len: u16,
    bytes: [u8; MAX_PACKET_PAYLOAD_SIZE]
}

impl Packet {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.bytes.len());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&self.payload_len.to_le_bytes());
        buf.extend_from_slice(&self.bytes[..self.payload_len as usize]); // only real data, no padding
        buf
    }

    fn from_bytes(data: &[u8]) -> Option<Packet> {
        if data.len() < 8 { return None; }
        let seq = u16::from_le_bytes([data[0], data[1]]);
        let timestamp = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        let payload_len = u16::from_le_bytes([data[6], data[7]]);
        if data.len() != 8 + payload_len as usize { return None; } // The package has incorrect length

        let mut bytes = [0u8; MAX_PACKET_PAYLOAD_SIZE];
        bytes[..payload_len as usize].copy_from_slice(&data[8..]);
        Some(Packet { seq, timestamp, payload_len, bytes })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Contact {
    username: String,
    ip: IpAddr,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ContactBook {
    contacts: Vec<Contact>,
}

impl ContactBook {
    fn add_contact(&mut self, contact: Contact) {
        self.contacts.push(contact);
    }
}

#[derive(Debug)]
struct Dialog {
    call_id: String,
    local_tag: String,
    remote_tag: Option<String>, // Only filled in once the callee's 200 OK has arrived
    peer_addr: IpAddr,
    peer_sip_port: u16,
    peer_media_port: Option<u16>, // From their SDP
    local_media_port: u16,
    cseq: u32,
}

fn get_local_ip() -> Result<IpAddr> {

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip())
}

fn print_help() {
    println!("\n#----------------------------------------------#");
    println!("Please provide if you are a client or server");
    println!("#----------------------------------------------#");
}

fn run_server() -> Result<()> {
    // Handle audio output
    let host: cpal::Host = cpal::default_host();
    let output_device: cpal::Device = host.default_output_device().expect("No output device available");
    let output_supported_config = output_device.default_output_config().expect("Error while querying output configs");
    let output_config: StreamConfig = output_supported_config.into();

    let output_channels = output_config.channels as usize;

    let mut jitter_arr: Vec<Packet> = Vec::new();

    let ring_buffer = HeapRb::<i16>::new(SAMPLE_RATE * CHANNELS_COUNT);
    let (mut producer, mut consumer) = ring_buffer.split();

    let output_stream = output_device.build_output_stream(
        output_config, 
        move |data: &mut [i16], _: &cpal::OutputCallbackInfo| {
            // React to stream events and read or write stream data here

            for frame in data.chunks_mut(output_channels) {
                let l = consumer.try_pop().unwrap_or(0) as i32;
                let r = consumer.try_pop().unwrap_or(0) as i32;
                let mixed = ((l + r) / 2) as i16;

                for channel_sample in frame.iter_mut() {
                    *channel_sample = mixed;
                }
            }

        }, 
        move |err| {
            // React to errors
            eprintln!("Output stream error: {}", err);
        }, 
        None 
        // Timeout for stream initialization: None = wait indefinitively. Some(Duration) = time to wait for the backend
    ).expect("Failed to unwrap the output stream");

    output_stream.play().expect("Failed to playback the audio");

    let local_ip = match get_local_ip() {
    Ok(ip) => Some(ip),
    Err(err) => {
        eprintln!("Error getting local IP: {:?}", err);
        None
    }
    }.expect("Could not get local ip address");


    let addr = format!("0.0.0.0:{}", DEFAULT_PORT);
    let socket = UdpSocket::bind(&addr)?;
    println!("This machine has started listening for UDP connections on: {}", addr);
    socket.set_nonblocking(true)?;

    thread::spawn(move || {
        let mut buf = [0u8; PACKET_TOTAL_SIZE];

        let channels = Channels::Stereo;
        let mut decoder = Decoder::new(SAMPLE_RATE as u32, channels).expect("Failed to init decoder");

        loop {
            match socket.recv_from(&mut buf) {
                Ok((len, src)) => {

                    println!("Received '{}' bytes from'{}'", len, src);
                    let packet: Packet = Packet::from_bytes(&buf[..len]).expect("Could not get packet form received buffer");
                    
                    if jitter_arr.len() >= 60 {
                        // Jitter array is full so we have to add some more sound to the output
                        let out_pac = jitter_arr.remove(0);

                        let mut decoded_buf = vec![0i16; MAX_PACKET_PAYLOAD_SIZE];
                        match decoder.decode(&out_pac.bytes[..out_pac.payload_len as usize], &mut decoded_buf, false) {
                            Ok(decoded_samples) => {
                                let sample_count = decoded_samples * CHANNELS_COUNT;
                                for &sample in &decoded_buf[..sample_count] {
                                    let _ = producer.try_push(sample);
                                }
                            }
                            Err(e) => {
                                eprintln!("Opus decode failed for seq {}, dropping: {}", out_pac.seq, e);
                                // Add pushing silence here
                            }
                        }
                    }

                    let insert_pos = jitter_arr.iter().position(|p| packet.seq < p.seq);

                    match insert_pos {
                        Some(index) => jitter_arr.insert(index, packet),
                        None => jitter_arr.push(packet)
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(e) => {
                    eprintln!("Error with receiving udp packet: {}", e);
                }
            }
        }
    });

    loop {
        thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

fn run_client(target_addr: String) -> Result<()> {

    // Handle audio input
    let host: cpal::Host = cpal::default_host();
    let input_device: cpal::Device = host.default_input_device().expect("No Input device available");

    let input_supported_config = input_device.default_input_config().expect("Error while querying input configs");
    let input_config: StreamConfig = input_supported_config.into();

    let addr: &str = "0.0.0.0:0";
    let socket = UdpSocket::bind(addr)?;
    socket.set_nonblocking(true)?;
    
    const TARGET_SAMPLES: usize = MAX_PACKET_PAYLOAD_SIZE / BYTES_PER_SAMPLE;


    let ring_buffer = HeapRb::<i16>::new(SAMPLE_RATE * CHANNELS_COUNT);
    let (mut producer, mut consumer) = ring_buffer.split();

    let input_stream = input_device.build_input_stream(
        input_config,
        move |data: &[i16], _: &cpal::InputCallbackInfo| {
            // Read stream input audio
            for &value in data {
                let _ = producer.try_push(value);
            }
        }, 
        move|err|{
            // React to errors
            eprintln!("Input stream error: {}", err);
        }, 
        None 
        // Timeout for stream initialization: None = wait indefinitively. Some(Duration) = time to wait for the backend
    ).expect("Failed to unwrap the input stream");



    thread::spawn(move || {
        // Make an array that is 4 sec long of 48000hz * 2 channels audio and split it so that one can push and one cat pop
        let mut chunk: Vec<i16> = Vec::with_capacity(TARGET_SAMPLES);

        let target_ip = format!("{}:{}", target_addr, DEFAULT_PORT);
        let target_addr: SocketAddr = target_ip.parse().expect("Failed to parse target address");
        println!("Ready to send to: {}", target_ip);

        let channels = input_config.channels as usize;
        let frames_per_packet = (TARGET_SAMPLES / channels) as u32;

        let mut sequencer: u16 = 0;
        let mut timer: u32 = 0;

        let channels = Channels::Stereo;
        let application = Application::Audio;

        let mut encoder = Encoder::new(SAMPLE_RATE as u32, channels, application).expect("Could not init encoder");

        loop {
            while chunk.len() < TARGET_SAMPLES {
                match consumer.try_pop() {
                    Some(sample) => {
                        chunk.push(sample)
                    },
                    None => thread::sleep(Duration::from_millis(1))
                }
            }

            let mut encoded_buf = [0u8; MAX_PACKET_PAYLOAD_SIZE];
            match encoder.encode(&chunk, &mut encoded_buf) {
                Ok(encoded_len) => {
                    let packet = Packet {
                    seq: sequencer,
                    timestamp: timer,
                    payload_len: encoded_len as u16,
                    bytes: encoded_buf
                    };

                    if let Err(e) = socket.send_to(&packet.to_bytes(), target_addr) {
                        eprintln!("Send error: {}", e);
                    }

                    // We've now sent another packet
                    sequencer += 1;
                    timer += frames_per_packet;
                }
                Err(e) => {
                    eprintln!("Opus encode failed, dropping this frame: {}", e);
                }
            }

            chunk.clear();
        }
    });

    input_stream.play().unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

fn print_contacts_from_book(book: &ContactBook) {
        if book.contacts.len() != 0 {
        println!("\nContacts in contact book: ");
        println!("==============================================");
        for contact  in book.contacts.iter() {
            println!("Username: \"{}\"    IP: \"{}\"", contact.username, contact.ip)
        }
        println!("==============================================");
    }
}

fn find_contact_from_username<'a>(username: &str, book: &'a ContactBook) -> Option<&'a Contact> {
    book.contacts.iter().find(|c| c.username.eq_ignore_ascii_case(username))
}

fn parse_media_port(body: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(body).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("m=audio ") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}


fn run_sip_server(local_contact: &Contact, calls: Arc<Mutex<HashMap<String, Dialog>>>) -> Result<()> {
    let local_addr = format!("0.0.0.0:{}", 55060);
    let socket = UdpSocket::bind(local_addr).expect("Could not bind to local socket");

    let mut buf = [0u8; 4096]; 

    loop {
        let (len, src) = socket.recv_from(&mut buf).expect("Failed to recv");
        match rsip::SipMessage::try_from(&buf[..len]) {
            Ok(rsip::SipMessage::Request(req)) => {
                let mut response_headers = rsip::Headers::default();
                response_headers.push(req.via_header().expect("Could not convert via_header").clone().into());
                response_headers.push(req.from_header().expect("Could not convert from_header").clone().into());
                response_headers.push(req.call_id_header().expect("Could not convert id_header").clone().into());
                response_headers.push(req.cseq_header().expect("Could not convert cseq_header").clone().into());
                
                let to_header = req.to_header().expect("Failed to_header the request").typed().expect("Failed typed to_header");

                match req.method {
                    rsip::Method::Invite => {
                        println!("We got an INVITE from {}: {}", src, req.uri);
                        let mut resp100_headers = response_headers.clone();
                        resp100_headers.push(rsip::Header::ContentLength(Default::default()));
                        resp100_headers.push(to_header.clone().into());

                        let resp_to_send = rsip::Response {
                            status_code: 100.into(),
                            headers: resp100_headers.clone(),
                            version: rsip::Version::V2,
                            body: vec![],
                        };

                        let socket = UdpSocket::bind("0.0.0.0:0").expect("Could not bind to local socket");
                        let target_addr = SocketAddr::new(src.ip(), 55060);
                        
                        let message: rsip::SipMessage = resp_to_send.into();
                        let wire_bytes = message.to_string();

                        if let Err(e) = socket.send_to(wire_bytes.as_bytes(), target_addr) {eprintln!("Failed to send message: {}", e);}
                        let mut resp180_headers = response_headers.clone();
                        resp180_headers.push(to_header.clone().into());
                        resp180_headers.push(rsip::Header::ContentLength(Default::default()));
                        let resp_to_send = rsip::Response {
                            status_code: 180.into(),
                            headers: resp180_headers.clone(),
                            version: rsip::Version::V2,
                            body: vec![],
                        };

                        let message: rsip::SipMessage = resp_to_send.into();
                        let wire_bytes = message.to_string();

                        if let Err(e) = socket.send_to(wire_bytes.as_bytes(), target_addr) {eprintln!("Failed to send message: {}", e);}
                        
                        println!("You are being called from {}. Auto accepting", src);
                        // println!("You are being called from {}. Do you want accept the call (y/n):", src);
                        // let mut answer = String::new();
                        // stdin().read_line(&mut answer).expect("Failed to read line");
                        // let answer = answer.trim();

                        let local_tag: u64 = rand::random();
                        let mut to_typed = req.to_header().expect("Failed to_header the request").typed().expect("Failed typed to_header");
                        to_typed.params.push(rsip::Param::Tag(local_tag.to_string().into()));

                        let sdp = build_sdp(local_contact.ip, DEFAULT_PORT);
                        let mut headers = response_headers.clone();
                        headers.push(to_typed.into());
                        headers.push(rsip::Header::ContentType("application/sdp".into()));
                        headers.push(rsip::Header::ContentLength((sdp.len() as u32).into()));

                        let resp_to_send = rsip::Response {
                            status_code: 200.into(),
                            headers: headers.clone(),
                            version: rsip::Version::V2,
                            body: sdp.into_bytes(),
                        };

                        let message: rsip::SipMessage = resp_to_send.into();
                        let wire_bytes = message.to_string();

                        if let Err(e) = socket.send_to(wire_bytes.as_bytes(), target_addr) {eprintln!("Failed to send message: {}", e);}
                    },
                    rsip::Method::Ack => {
                        println!("Got ACK from {}: {}", src, req.uri);
                        // Call is now established
                        let caller_ip = src.ip().to_string();
                        std::thread::spawn(move || {
                            if let Err(e) = run_client(caller_ip) {
                                eprintln!("UDP client error: {}", e);
                            }
                        });
                    },
                    other => {
                        println!("Got unhandled request method {} from {}", other, src);
                    }
                }
            },
            Ok(rsip::SipMessage::Response(resp)) => {
                match resp.status_code {
                    rsip::StatusCode::Trying => {
                        println!("100 Trying from {}", src);
                    },
                    rsip::StatusCode::Ringing => {
                        println!("180 Ringing from {}", src);
                        println!("The ringtone at {} is going of 'in spirit'", src);
                    },
                    rsip::StatusCode::OK => {
                        println!("200 OK from {}", src);
                        let calls = calls.lock().expect("Cannot lock calls");
                        let call_id = resp.call_id_header().expect("Could not get call_id from resp header").value().to_string();
                        let dialog = calls.get(&call_id).expect("Could not get dialog");
                        let local_uri_with_tag = format!("<sip:{}@{}>;tag={}", local_contact.username, local_contact.ip, dialog.local_tag);
                        let to_header = resp.to_header().expect("Failed to_header the request").typed().expect("Failed typed to_header");
                        let ack_branch: u64 = rand::random();
                        let via = format!("SIP/2.0/UDP {}:55060;branch=z9hG4bK{}", local_contact.ip, ack_branch);

                        let mut headers = rsip::Headers::default();
                        headers.push(From::new(&local_uri_with_tag).into());
                        headers.push(to_header.into());
                        headers.push(Via::new(&via).into());
                        headers.push(CallId::new(dialog.call_id.clone()).into());
                        headers.push(CSeq::new(format!("{} ACK", dialog.cseq)).into());

                        let remote_uri = rsip::Uri {
                            scheme: Some(rsip::Scheme::Sip),
                            auth: None,
                            host_with_port: rsip::HostWithPort {
                                host: rsip::Host::IpAddr(dialog.peer_addr),
                                port: Some(dialog.peer_sip_port.into()),
                            },
                            ..Default::default()
                        };

                        let ack = Request {
                            method: Method::Ack,
                            uri: remote_uri,
                            version: Version::V2,
                            headers,
                            body: vec![],
                        };

                        let target_addr = SocketAddr::new(src.ip(), 55060);
                        let message: rsip::SipMessage = ack.into();
                        let wire_bytes = message.to_string();

                        if let Err(e) = socket.send_to(wire_bytes.as_bytes(), target_addr) {eprintln!("Failed to send message: {}", e);}

                        std::thread::spawn(move || {
                            if let Err(e) = run_client(src.ip().to_string()) {
                                eprintln!("UDP client error: {}", e);
                            }
                        });

                    },
                    other => {
                        println!("Got unhandled status {:?} from {}", other, src);
                    }
                }
            },
            Err(err) => eprintln!("Failed to parse SIP message from {}: {}", src, err),
        };
    }

    Ok(())
}

fn build_sdp(local_ip: IpAddr, media_port: u16) -> String {
    format!(
        "v=0\r\n\
            o=- 0 0 IN IP4 {ip}\r\n\
            s=voip-test\r\n\
            c=IN IP4 {ip}\r\n\
            t=0 0\r\n\
            m=audio {port} RTP/AVP 96\r\n\
            a=rtpmap:96 opus/48000/2\r\n",
        ip = local_ip, port = media_port
    )
}

fn call_contact(local_contact: &Contact, target_contact: &Contact, calls: Arc<Mutex<HashMap<String, Dialog>>>) {
    let mut headers = rsip::Headers::default();

    let branch: u64 = rand::random();
    let call_id: u64 = rand::random();
    let tag: u64 = rand::random();


    let local_uri = format!("<sip:{}@{}>;tag={}", local_contact.username, local_contact.ip, tag);
    headers.push(From::new(&local_uri).into());

    let remote_uri = format!("<sip:{}@{}>", target_contact.username, target_contact.ip);
    headers.push(To::new(&remote_uri).into());

    let via = format!("SIP/2.0/UDP {}:55060;branch=z9hG4bK{}", local_contact.ip, branch);
    headers.push(Via::new(&via).into());

    headers.push(CallId::new(format!("{}", call_id)).into());

    headers.push(CSeq::new("1 INVITE").into());

    headers.push(MaxForwards::new("70").into());


    let mut calls = calls.lock().expect("Cannot lock calls");

    calls.insert(call_id.to_string(), Dialog {
        call_id: call_id.to_string(),
        local_tag: tag.to_string(),
        remote_tag: None,
        peer_addr: target_contact.ip,
        peer_sip_port: 55060,
        peer_media_port: None,
        local_media_port: 9999,
        cseq: 1,
    });

    let remote_uri = rsip::Uri {
        scheme: Some(rsip::Scheme::Sip),
        auth: Some((target_contact.username.as_str(), Option::<String>::None).into()),
        host_with_port: rsip::HostWithPort {
            host: rsip::Host::IpAddr(target_contact.ip),
            port: Some(55060.into()),
        },
        ..Default::default()
    };

    let sdp = build_sdp(local_contact.ip, DEFAULT_PORT as u16);
    headers.push(rsip::Header::ContentType("application/sdp".into()));
    headers.push(rsip::Header::ContentLength((sdp.len() as u32).into()));

    let request = Request {
        method: Method::Invite,
        uri: remote_uri,
        version: Version::V2,
        headers,
        body: sdp.into_bytes(),
    };

    let socket = UdpSocket::bind("0.0.0.0:0").expect("Could not bind to local socket");

    let target_addr = SocketAddr::new(target_contact.ip, 55060);
    
    let message: rsip::SipMessage = request.into();
    let wire_bytes = message.to_string();

    if let Err(e) = socket.send_to(wire_bytes.as_bytes(), target_addr) {eprintln!("Failed to send message: {}", e);}
}

fn main() -> Result<()> {
    let mut contact_book: ContactBook = match File::open("ContactBook.json") {
        Ok(file) => {
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).unwrap_or_else(|e| {
                eprintln!("Error reading from ContactBook.json: {:?}", e);
                ContactBook::default()
            })
        }
        Err(_) => ContactBook::default(), // file doesn't exist yet — start fresh
    };
    
    let mut input = String::new();

    std::thread::spawn(|| {
        if let Err(e) = run_server() {
            eprintln!("UDP server error: {}", e);
        }
    });
    thread::sleep(Duration::from_millis(100));

    println!("\nWho from your contact list are you?");
    print_contacts_from_book(&contact_book);
    print!("Username: ");
    stdout().flush().unwrap(); 
    let mut local_user = String::new();
    stdin().read_line(&mut local_user).expect("Failed to read line");
    let local_user = local_user.trim();
    let local_contact = match find_contact_from_username(&local_user, &contact_book) {
        Some(c) => c.clone(),
        None => {
            eprintln!("Could not find the contact in the contact book");
            return Ok(());
        }
    };

    let calls: Arc<Mutex<HashMap<String, Dialog>>> = Arc::new(Mutex::new(HashMap::new()));
    let local_for_sip_serv = local_contact.clone();
    let server_call_clone  = Arc::clone(&calls);
    std::thread::spawn(move || {
        if let Err(e) = run_sip_server(&local_for_sip_serv, server_call_clone) {
            eprintln!("Sip server error: {}", e);
        }
    });


    loop  {
        input.clear();
        print_contacts_from_book(&contact_book);
        println!("You can now move on with this contact list, or add more contacts");
        println!("Enter a command (add / done / call): ");
        stdin().read_line(&mut input).expect("Failed to read line");
        let input = input.trim();

        if input.eq_ignore_ascii_case("done") || input.eq_ignore_ascii_case("quit") {
            break;
        } else if input.eq_ignore_ascii_case("add") {
            let mut username_inp = String::new();
            print!("Username of contact: ");
            stdout().flush().unwrap(); 
            stdin().read_line(&mut username_inp).expect("Failed to read line");
            let username = username_inp.trim().to_string();

            let mut ip_inp = String::new();
            print!("Public IP of contact: ");
            stdout().flush().unwrap(); 
            stdin().read_line(&mut ip_inp).expect("Failed to read line");
            let ip = IpAddr::from_str(ip_inp.trim()).expect("Could not parse provided ip address");

            let new_contact = Contact {username, ip};

            contact_book.add_contact(new_contact);
        } else if input.eq_ignore_ascii_case("call") {
            println!("Who from you contact list would you like to call, input their username?");
            print_contacts_from_book(&contact_book);
            print!("\nUsername: ");
            stdout().flush().unwrap(); 
            let mut username_to_call = String::new();
            stdin().read_line(&mut username_to_call).expect("Failed to read line");
            let username_to_call = username_to_call.trim();

            let contact_to_call = match find_contact_from_username(&username_to_call, &contact_book) {
                Some(c) => c,
                None => {
                    eprintln!("Could not find the contact in the contact book");
                    return Ok(());
                }
            };

            let call_clone  = Arc::clone(&calls);

            call_contact(&local_contact, contact_to_call, call_clone);
            break;
        }
    }

    let write_file = File::create("ContactBook.json")?;
    let writer = BufWriter::new(write_file);
    if let Err(e) = serde_json::to_writer_pretty(writer, &contact_book) {
        eprintln!("Could not save contact book: {}", e);
    }
    loop {
            thread::sleep(Duration::from_secs(1));
    }


    // Implement the following RSIP messages to init a call

    // INVITE: a caller invites a person they want to speak with                                                                | caller -> invitee
    // 100 Trying: the intvitee sends this back when they receive invite telling the caller they're looking for them            | invitee -> caller
    // 180 Ringing: The invitees phone starts ringing, and the response goes back to the caller to tell them the call has begun | invitee -> caller
    // 200 OK: The invitee picks up the call and the call is initiated                                                          | invitee -> caller
    // ACK: The caller acks the 200 OK from the invitee                                                                         | caller -> invitee



    Ok(())
}