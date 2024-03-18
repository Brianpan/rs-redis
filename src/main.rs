use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {
    let mut cmd = String::new();
    let resp = "+PONG\r\n";
    let mut buf = [0; 512];

    loop {
        let chrs = stream.read(&mut buf);
        match chrs {
            Ok(n) => {
                if n == 0 {
                    break;
                } else {
                    let mut maybe_end = false;
                    while let Some(u) = buf.first() {
                        let c = *u as char;
                        if c == '\r' {
                            maybe_end = true;
                        } else if c == '\n' {
                            if !maybe_end {
                                break;
                            }
                            let check_cmd = cmd.to_lowercase();
                            if check_cmd == "ping" {
                                stream.write_all(resp.as_bytes()).unwrap();
                            }

                            maybe_end = false;
                            cmd.clear();
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
