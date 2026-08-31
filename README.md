# Architecture

## Audio 
Use `cpal` for low-level audio capture and playback.  
[https://docs.rs/cpal/latest/cpal/](https://docs.rs/cpal/latest/cpal/)

## Networking
Starting with `std::net::UdpSocket`, but considering using `tokio`  
[https://docs.rs/tokio/latest/tokio/](https://docs.rs/tokio/latest/tokio/)

## Packaging
Building the jitterbuffer and reordering logic myself to begin with to learn how it works, then perhaps moving to `webrtc-rs`  
[https://github.com/webrtc-rs/webrtc](https://github.com/webrtc-rs/webrtc)

## Codec
Starting with raw uncompressed PCM, and then adding Opus through `audiopus` or `opus`  
[https://opus-codec.org/](https://opus-codec.org/)  
[https://crates.io/crates/audiopus](https://crates.io/crates/audiopus)  
[https://crates.io/crates/opus](https://crates.io/crates/opus)

## x509/identity
Generating x509s with `rcgen`, using `rsustls` for signaling and handshake, and `rustls-pemfile` for parsing certs.  
[https://docs.rs/rcgen/latest/rcgen/](https://docs.rs/rcgen/latest/rcgen/)  
[https://docs.rs/rustls/latest/rustls/](https://docs.rs/rustls/latest/rustls/)  
[https://crates.io/crates/rustls-pemfile](https://crates.io/crates/rustls-pemfile)

## Encryption
While real VOIP uses DTLS-SRTP over UDP i'll simplify and use a symmetric key exchanged over TCP to encrypt the packages with `chacha20poly1305` or `ring`  
[https://docs.rs/chacha20poly1305/latest/chacha20poly1305/](https://docs.rs/chacha20poly1305/latest/chacha20poly1305/)  
[https://crates.io/crates/ring](https://crates.io/crates/ring)

## Storage
`serde` and `serde_json` for simple contacts file.  
[https://docs.rs/serde/latest/serde/](https://docs.rs/serde/latest/serde/)  
[https://docs.rs/serde_json/latest/serde_json/](https://docs.rs/serde_json/latest/serde_json/)

## CLI
Use `clap` for argument parsing and subcommands (add-contact, call, listen) and so on.  
[https://crates.io/crates/clap](https://crates.io/crates/clap)
