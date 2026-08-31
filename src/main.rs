use std::{io::Result, thread, time::Duration};
use cpal::{StreamConfig, traits::{DeviceTrait, HostTrait, StreamTrait}};
use ringbuf::{HeapRb, traits::*};


// struct User {
//     username: String,
//     ip: String,
// }




fn main() -> Result<()> {
    let host: cpal::Host = cpal::default_host();
    let input_device: cpal::Device = host.default_input_device().expect("No Input device available");

    let input_supported_config = input_device.default_input_config().expect("Error while querying input configs");
    let input_config: StreamConfig = input_supported_config.into();

    // Make an array that is 4 sec long of 48000hz audio and split it so that one can push and one cat pop
    let rb = HeapRb::<i16>::new(48000 * 8);
    let (mut producer, mut consumer) = rb.split();

    println!("You can now speak to yourself for 10 sec");
    let input_stream = input_device.build_input_stream(
        input_config, 
        move |data: &[i16], _: &cpal::InputCallbackInfo| {
            // Read stream input audio
            for &value in data {
                producer.try_push(value).unwrap();
            }

        }, 
        move|err|{
            // React to errors
            eprintln!("Input stream error: {}", err);
        }, 
        None 
        // Timeout for stream initialization: None = wait indefinitively. Some(Duration) = time to wait for the backend
    ).expect("Failed to unwrap the input stream");

    input_stream.play().expect("Failed to start recording input audio");

        
    let output_device: cpal::Device = host.default_output_device().expect("No output device available");
    let output_supported_config = output_device.default_output_config().expect("Error while querying output configs");
    let output_config: StreamConfig = output_supported_config.into();


    let output_channels = output_config.channels as usize;

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

    thread::sleep(Duration::from_secs(10));
    drop(output_stream);
    Ok(())
}










// fn main()  -> Result<()> {
//     print!("Choose a username: ");
//     stdout().flush().unwrap();  

//     let mut input: String = String::new();
//     stdin().read_line(&mut input).expect("Failed to read line");

//     println!("Chosen username is \"{}\"", input);
//     let user_ip = match local_ip() {
//         Ok(ip) => Some(ip),
//         Err(err) => {
//             eprintln!("Error getting local IP: {:?}", err);
//             None
//         }
//     };
//     println!("Local ip is {:?}",user_ip);

//     /* This is handling listening for incomming connections and calling handle_client_connect for them */
//     let addr: &str = "0.0.0.0:9999";
//     let listener: TcpListener = TcpListener::bind(addr)?;
//     println!("Now listening for connections on port 9999");

//     let read_handle = thread::spawn(move || -> Result<()> { 
//         for stream in listener.incoming() {
//             let read_stream: TcpStream = stream?;
//             let mut line: String = String::new();
//             let mut reader: BufReader<TcpStream> = BufReader::new(read_stream);
//             loop {
//                 match reader.read_line(&mut line) {
//                     Ok(0) => {
//                         println!("Closed the connection");
//                         break;
//                     }
//                     Ok(_) => {
//                         print!("\nReceived: {}", line);
//                         print!("Message: ");
//                         stdout().flush().unwrap();    
//                     }
//                     Err(e) => {
//                         eprintln!("Read error: {}", e);
//                         break;
//                     }
//                 }
//             }
//         }
//         Ok(())
//     });
    
//     /* This should be for connecting to others */
//     println!("What ip do you want to speak to?");
//     print!("Input target IP: ");
//     stdout().flush().unwrap();

//     let mut target_ip: String = String::new();
//     stdin().read_line(&mut target_ip).expect("Failed to read line");
//     let tmp_ip = target_ip.trim();
//     target_ip = format!("{}:9999", tmp_ip);
//     let target_ip: &str = &target_ip;

//     println!("Trying to connect to {}", target_ip);
//     let mut stream = TcpStream::connect(target_ip)?;
//     println!("Type \"q\" or \"quit\" to stop sending messages");

//     let mut message: String = String::new();

//     while !message.trim().eq_ignore_ascii_case("quit") && !message.trim().eq_ignore_ascii_case("q") {
//         message.clear();
//         print!("Message: ");
//         stdout().flush().unwrap();
//         stdin().read_line(&mut message).expect("Failed to read line");

//         let byte_message: &[u8] = message.as_bytes();
//         stream.write_all(byte_message)?;
//     }
    
//     let _ = read_handle.join();
//     Ok(())
// }


// fn handle_client_connect(stream: TcpStream) -> Result<()>{
//     println!("Peer address is {}", stream.peer_addr()?);

    
//     Ok(())
// }
// fn main() {
//     let args: Vec<String> = env::args().collect();
//     let mut job: String =  String::new();

//     if args.len() > 1 {
//         job = args[1].to_lowercase();
//     } else {
//         print_help();
//     }

//     if job == "-h" || job == "help" {
//         print_help();
//     } else if job == "client" {
//         if let Err(e) = run_client() { eprintln!("Client error: {}", e); }
//     } else if job == "server" {
//         if let Err(e) = run_server() { eprintln!("Server error: {}", e); }
//     }
// }

// fn print_help() {
//     println!("#----------------------------------------------#");
//     println!("Please provide if you are a client or server");
//     println!("#----------------------------------------------#");
// }

// /* SERVER SIDE LOGIC */

// fn run_server() -> Result<()> {
//     println!("I am a server");
//     let addr: &str = "0.0.0.0:9999";
    
//     let listener = TcpListener::bind(addr)?;
//     println!("Listening to {}", addr);

//     for stream in listener.incoming() {
//         handle_client_connect(stream?)?;
//     }

//     Ok(())
// }


// fn handle_client_connect(mut stream: TcpStream)  -> Result<()> {
//     let read_stream: TcpStream = stream.try_clone()?;
//     let mut write_stream: TcpStream = stream;

//     let read_handle = thread::spawn(move || {
//         let mut reader: BufReader<TcpStream> = BufReader::new(read_stream);
//         loop {
//             let mut line: String = String::new();
//             match reader.read_line(&mut line) {
//                 Ok(0) => {
//                     println!("Closed the connection");
//                     break;
//                 }
//                 Ok(_) => {
//                     print!("\nReceived: {}", line);
//                     print!("Message: ");
//                     stdout().flush().unwrap();    
//                 }
//                 Err(e) => {
//                     eprintln!("Read error: {}", e);
//                     break;
//                 }
//             }
//         }
//     });

//     loop {
//         print!("Message: ");
//         stdout().flush().unwrap();

//         let mut message = String::new();
//         stdin().read_line(&mut message).expect("Failed to read line");
//         let trimmed = message.trim();

//         if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("q") {
//             break;
//         }

//         write_stream.write_all(format!("{}\n", trimmed).as_bytes())?;
//     }
    
//     let _ = read_handle.join();
//     Ok(())
// }

// /* CLIENT SIDE LOGIC */

// fn run_client()  -> Result<()> {
//     println!("I am a client");

//     println!("What ip do you want to speak to?");
//     print!("Format = \"<IP>:<PORT>\": ");
//     stdout().flush().unwrap();

//     let mut target_ip: String = String::new();
//     stdin().read_line(&mut target_ip).expect("Failed to read line");
//     let target_ip = target_ip.trim();

//     println!("Trying to connect to {}", target_ip);
//     let mut stream = TcpStream::connect(target_ip)?;
//     println!("Type \"q\" or \"quit\" to stop sending messages");

//     let mut message: String = String::new();

//     while !message.trim().eq_ignore_ascii_case("quit") && !message.trim().eq_ignore_ascii_case("q") {
//         message.clear();
//         print!("Message: ");
//         stdout().flush().unwrap();
//         stdin().read_line(&mut message).expect("Failed to read line");

//         let byte_message: &[u8] = message.trim().as_bytes();
//         stream.write_all(byte_message)?;
//     }
    
//     Ok(())
// }


    


// fn main() {
//     // Connect to this computers interface

//     let interfaces: Vec<datalink::NetworkInterface> = datalink::interfaces();
//     let interface = interfaces.into_iter().find(|iface| !iface.is_loopback()).expect("No non-loopback interface found");
//     println!("Since {} is not loopback we will use this interface", interface);

// }


// struct Client {
//     name: String,
//     ip: String,
// }

// fn main() {
//     //let mut clients: Vec<Client> = Vec::new();

    
//     print!("What is your name: ");
//     io::stdout().flush().unwrap();
//     let mut input = String::new();
//     io::stdin().read_line(&mut input).expect("Failed to read line");

//     let trimmed = input.trim();
//     println!("nice to meet you {}", trimmed);
// }
