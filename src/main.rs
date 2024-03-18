use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {
    let mut cmd = String::new();
    let resp = "+PONG\r\n";
    let mut buf = [0; 1024];

    loop {
        let chrs = stream.read(&mut buf);
        match chrs {
            Ok(n) => {
                if n == 0 {
                    break;
                } else {
                    for u in buf.iter().take(n) {
                        let c = *u as char;

                        if c == '\n' {
                            let check_cmd = cmd.to_lowercase();
                            if check_cmd == "ping" {
                                stream.write_all(resp.as_bytes()).unwrap();
                            }

                            cmd = String::new();
                        } else {
                            cmd.push(c);
                        }
                    }

                    if !cmd.is_empty() {
                        let check_cmd = cmd.to_lowercase();
                        if check_cmd == "ping" {
                            stream.write_all(resp.as_bytes()).unwrap();
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

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
    // Uncomment this block to pass the first stage
    //
    // let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    // for stream in listener.incoming() {
    //     match stream {
    //         Ok(_stream) => {
    //             println!("accepted new connection");
    //         }
    //         Err(e) => {
    //             println!("error: {}", e);
    //         }
    //     }
    // }
}
