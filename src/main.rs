use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {
    let resp = "+PONG\r\n";

    stream.write_all(resp.as_bytes()).unwrap();
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("new connection");
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
