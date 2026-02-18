//! Client for the TRDB server

use std::env;
use std::time::Duration;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// This is just a hardcoded client used for testing the communication with the server
// but this crate will basically contain the CLI client that will read queries from the user,
// send them to the server and print back the result.

async fn send_and_receive(
    stream: &mut TcpStream,
    value: u32,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Send the u32
    let bytes = value.to_le_bytes();
    stream.write_all(&bytes).await?;
    eprintln!("Sent {} (bytes: {:?})", value, bytes);

    // Read response - up to 1024 bytes
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    eprintln!("read returned: {}", n);
    if n == 0 {
        // connection closed by server; return empty Vec to signal closed connection
        return Ok(Vec::new());
    }
    buf.truncate(n);
    Ok(buf)
}

fn print_result(result: &[u8]) {
    // Show number of bytes and a hex preview
    let len = result.len();
    // hex preview for first up to 32 bytes
    let preview_len = std::cmp::min(32, len);
    let hex_preview: String = result[..preview_len]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");

    println!(
        "Received ({} bytes) - hex[{}/{}]: {}",
        len, preview_len, len, hex_preview
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // If a cmd line arg was provided, do a single send/receive and exit
    if let Some(arg) = env::args().nth(1) {
        let value: u32 = arg.trim().parse()?;

        // Connect to the server
        let mut stream = TcpStream::connect("127.0.0.1:8080").await?;

        let result = send_and_receive(&mut stream, value).await?;
        if result.is_empty() {
            eprintln!(
                "Server closed the connection before sending data. Attempting one reconnect/resend..."
            );
            // Try reconnect once
            match TcpStream::connect("127.0.0.1:8080").await {
                Ok(mut new_stream) => {
                    // small backoff to give server time
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    match send_and_receive(&mut new_stream, value).await {
                        Ok(r2) => {
                            if r2.is_empty() {
                                eprintln!("Server sent nothing after reconnect either");
                            } else {
                                print_result(&r2);
                            }
                        }
                        Err(e) => eprintln!("Error on resend after reconnect: {}", e),
                    }
                }
                Err(e) => eprintln!("Reconnect failed: {}", e),
            }
        } else {
            print_result(&result);
        }

        return Ok(());
    }

    // Otherwise start an interactive loop: try to connect once, then ask for input repeatedly
    // Note: the server currently handles one request per connection and then closes it. To
    // keep the interactive UX "connect once then loop", we will reconnect transparently if
    // the server closes the connection after serving a request, and retry the current value once.
    let mut stream_opt = match TcpStream::connect("127.0.0.1:8080").await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!(
                "Warning: couldn't connect at startup: {}. Will try on first command.",
                e
            );
            None
        }
    };

    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    println!("Interactive mode. Connected to server at 127.0.0.1:8080 (or will connect on demand)");

    loop {
        println!("Enter a number to send to the server (or 'quit' to exit):");

        let line = match lines.next_line().await? {
            Some(l) => l,
            None => break, // stdin closed
        };

        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }
        if trimmed.is_empty() {
            continue;
        }

        let value = match trimmed.parse::<u32>() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Invalid number: {} ({})", trimmed, e);
                continue;
            }
        };

        // Ensure we have a connection. If not, try to connect now.
        if stream_opt.is_none() {
            match TcpStream::connect("127.0.0.1:8080").await {
                Ok(s) => {
                    stream_opt = Some(s);
                    println!("Reconnected to server");
                }
                Err(e) => {
                    eprintln!("Failed to connect to server: {}", e);
                    // Ask for next input instead of exiting; user can try again
                    continue;
                }
            }
        }

        // At this point we have Some(stream)
        if let Some(mut stream) = stream_opt.take() {
            // Try sending once
            match send_and_receive(&mut stream, value).await {
                Ok(result) => {
                    if result.is_empty() {
                        // Server closed connection after serving the request. Try to reconnect and resend once.
                        eprintln!(
                            "Server closed the connection; attempting reconnect and resend once"
                        );
                        match TcpStream::connect("127.0.0.1:8080").await {
                            Ok(mut new_stream) => match send_and_receive(&mut new_stream, value)
                                .await
                            {
                                Ok(r2) => {
                                    if r2.is_empty() {
                                        eprintln!(
                                            "Server closed connection immediately after reconnect; giving up on this value"
                                        );
                                        // Keep stream_opt as None; continue to next input
                                        stream_opt = None;
                                        continue;
                                    } else {
                                        print_result(&r2);
                                        // keep the new connection for subsequent requests
                                        stream_opt = Some(new_stream);
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error after reconnect when sending value: {}", e);
                                    stream_opt = None;
                                    continue;
                                }
                            },
                            Err(e) => {
                                eprintln!("Reconnect failed: {}", e);
                                stream_opt = None;
                                continue;
                            }
                        }
                    } else {
                        print_result(&result);
                        // keep the current connection for further requests
                        stream_opt = Some(stream);
                        continue;
                    }
                }
                Err(e) => {
                    eprintln!("Error communicating with server: {}", e);
                    // drop this stream and let next loop attempt to reconnect
                    stream_opt = None;
                    continue;
                }
            }
        }
    }

    Ok(())
}
