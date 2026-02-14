//! Client for the TRDB server

use std::env;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// This is just a hardcoded client used for testing the communication with the server
// but this crate will basically contain the CLI client that will read queries from the user,
// send them to the server and print back the result.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // If a cmd line arg was provided, use that one
    let value: u32 = if let Some(arg) = env::args().nth(1) {
        arg.trim().parse()?
    } else {
        // Otherwise read it from stdin
        let mut stdin = io::BufReader::new(io::stdin());
        let mut input = String::new();
        println!("Enter a number to send to the server:");
        stdin.read_line(&mut input).await?;
        input.trim().parse()?
    };

    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;

    // Send the u32
    stream.write_all(&value.to_le_bytes()).await?;

    // Read response - up to 1024 bytes
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let result = &buf[..n];

    println!("Received: {:?}", result);

    Ok(())
}
