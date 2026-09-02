use std::{env, fs::File, io::{BufReader, Result, Write, stdin, stdout}, net::{IpAddr, SocketAddr, UdpSocket}, str::FromStr, thread, time::Duration};
use cpal::{StreamConfig, traits::{DeviceTrait, HostTrait, StreamTrait}};
use ringbuf::{HeapRb, traits::*};
use opus::{Application, Channels, Encoder, Decoder};
use rsip::param::user;
use serde::{Deserialize, Serialize};


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

#[derive(Serialize, Deserialize, Debug)]
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


fn get_local_ip() -> Result<IpAddr> {

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip())
}

fn print_help() {
    println!("#----------------------------------------------#");
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

    println!("Your local ip is {}",local_ip);


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

fn run_client() -> Result<()> {
    let local_ip = match get_local_ip() {
        Ok(ip) => Some(ip),
        Err(err) => {
            eprintln!("Error getting local IP: {:?}", err);
            None
        }
    }.expect("Could not get local ip address");
    
    println!("Your local ip is {}",local_ip);


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

        let mut target_ip = String::new();
        println!("What is the ip of the host you want to connect to?");
        stdin().read_line(&mut target_ip).expect("Failed to read line");
        let tmp_ip = target_ip.trim();
        target_ip = format!("{}:{}", tmp_ip, DEFAULT_PORT);
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
        println!("Contacts in contact book");
        for contact  in book.contacts.iter() {
            println!("Contact with name {} and IP {}", contact.username, contact.ip)
        }
    }
}

fn main() -> Result<()> {
    let file = File::open("ContactBook.json")?;
    let reader = BufReader::new(file);
    
    let mut contact_book: ContactBook = match serde_json::from_reader(reader) {
        Ok(data) =>  data,
        Err(e) => {eprintln!(
            "Error reading from ContactBook.json: {:?}", e);
            ContactBook::default()
        }
    };
    
    let mut input = String::new();

    loop  {
        input.clear();
        stdin().read_line(&mut input).expect("Failed to read line");
        let input = input.trim();
        print!("Enter a command (add / done): ");
        stdout().flush().unwrap();

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
        }

        print_contacts_from_book(&contact_book);
        println!("You can now move on with this contact list, or add more contacts");
        println!("Move on = \"done\", add contact \"add\"");
    }

    let args: Vec<String> = env::args().collect();
    let mut job: String =  String::new();


    if args.len() == 2 {
        job = args[1].to_lowercase();
    } else {
        print_help();
    }

    if job == "-h" || job == "help" {
        print_help();
    } else if job == "server" {
        if let Err(e) = run_server() { eprintln!("Server error: {}", e); }
    } else if job == "client" {
        if let Err(e) = run_client() { eprintln!("Client error: {}", e); }
    }

    Ok(())
}