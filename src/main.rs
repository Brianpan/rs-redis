use anyhow::Result;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn command_handler(cmd: &str) -> Result<String> {
    match cmd.to_lowercase().as_str() {
        "ping" => Ok("+PONG\r\n".to_string()),
        _ => Ok("".to_string()),
    }
}
async fn handle_connection(mut stream: TcpStream) {
    let mut cmd = String::new();
    let mut buf = [0; 512];

    loop {
        let chrs = stream.read(&mut buf).await;
        match chrs {
            Ok(n) => {
                if n == 0 {
                    break;
                } else {
                    for u in buf.iter().take(n) {
                        let c = *u as char;
                        if c == '\r' {
                            continue;
                        } else if c == '\n' {
                            if let Ok(resp) = command_handler(&cmd) {
                                stream.write_all(resp.as_bytes()).await.unwrap();
                            }

                            cmd.clear();
                        } else {
                            cmd.push(c);
                        }
                    }

                    if !cmd.is_empty() {
                        if let Ok(resp) = command_handler(&cmd) {
                            stream.write_all(resp.as_bytes()).await.unwrap();
                        }
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(socket).await;
        });
    }
}
