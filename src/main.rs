use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(PartialEq, Clone)]
enum RespParsingState {
    ParsingMeta,
    ParsingData,
    End,
}

#[derive(PartialEq, Clone)]
enum RespType {
    Unknown,
    Integer,
    Error,
    SimpleString,
    BulkString,
    Array,
}
#[derive(PartialEq, Clone)]
struct RespMessage {
    resp_type: RespType,
    state: RespParsingState,
    int_data: i64,
    str_data: String,
    vec_data: Vec<RespMessage>,
}

impl RespMessage {
    fn new() -> Self {
        RespMessage {
            resp_type: RespType::Unknown,
            state: RespParsingState::ParsingMeta,
            int_data: 0,
            str_data: String::new(),
            vec_data: Vec::new(),
        }
    }

    fn parse(&mut self, data: &str) {
        let iter = data.chars().peekable();
        for c in iter {
            match c {
                '+' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::SimpleString;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '-' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Error;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                ':' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Integer;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '$' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::BulkString;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '*' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Array;
                    } else {
                        self.str_data.push(c);
                    }
                }
                _ => {
                    // parsing logic
                    match self.resp_type {
                        RespType::SimpleString | RespType::Error => {
                            if self.state == RespParsingState::ParsingData {
                                self.str_data.push(c);
                            }
                        }
                        RespType::Integer => {
                            if self.state == RespParsingState::ParsingData {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            }
                        }
                        RespType::BulkString => {
                            if self.state == RespParsingState::ParsingMeta {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            } else if self.state == RespParsingState::ParsingData
                                && self.int_data > 0
                            {
                                self.str_data.push(c);
                                self.int_data -= 1;
                            }
                        }
                        RespType::Array => {
                            if self.state == RespParsingState::ParsingMeta {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            } else if self.state == RespParsingState::ParsingData {
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if self.state == RespParsingState::ParsingData {
            self.state = RespParsingState::End;
        } else if self.state == RespParsingState::ParsingMeta {
            self.state = RespParsingState::ParsingData;
        }
    }
}

fn command_handler(cmd: Arc<RwLock<RespMessage>>) -> Result<String> {
    let ret = match cmd.read().unwrap().resp_type {
        RespType::SimpleString => Ok("+OK\r\n".to_string()),
        RespType::Error => Ok("-ERR\r\n".to_string()),
        RespType::Integer => Ok(format!(":{}\r\n", cmd.read().unwrap().int_data)),
        RespType::BulkString => Ok(format!(
            "${}\r\n{}\r\n",
            cmd.read().unwrap().str_data.len(),
            "PONG"
        )),
        RespType::Array => {
            // TODO
            Ok("*0\r\n".to_string())
        }
        _ => Err(anyhow::anyhow!("Unknown command")),
    };

    ret
}

async fn handle_connection(mut stream: TcpStream) {
    let mut cmd = String::new();
    let mut buf = [0; 512];
    let mut maybe_split = false;

    // stack to handle nested commands
    let mut cmd_stack: VecDeque<Arc<RwLock<RespMessage>>> = VecDeque::new();

    // push the first element
    // this cmd stack is used to track nested commands specifically for array type
    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new())));

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
                            maybe_split = true;
                            continue;
                        } else if c == '\n' {
                            if !maybe_split {
                                cmd.push(c);
                                continue;
                            }

                            // logic to generate RespMessage
                            if let Some(resp) = cmd_stack.pop_back() {
                                resp.write().unwrap().parse(&cmd);

                                if resp.read().unwrap().state == RespParsingState::End {
                                    // cmd_stack.pop_back();
                                    if let Some(parent) = cmd_stack.back() {
                                        if parent.read().unwrap().int_data > 0 {
                                            parent.write().unwrap().int_data -= 1;
                                            parent
                                                .write()
                                                .unwrap()
                                                .vec_data
                                                .push(resp.read().unwrap().clone());
                                        }

                                        if parent.read().unwrap().int_data == 0 {
                                            // move the array type out of the stack
                                            // cmd_stack.pop_back();
                                            parent.write().unwrap().state = RespParsingState::End;
                                            if cmd_stack.len() == 0 {
                                                let resp = command_handler(parent.clone());
                                                if let Ok(resp) = resp {
                                                    stream
                                                        .write_all(resp.as_bytes())
                                                        .await
                                                        .unwrap();
                                                }
                                            }
                                        }
                                    } else if let Ok(resp) = command_handler(resp) {
                                        stream.write_all(resp.as_bytes()).await.unwrap();
                                    }

                                    // next cmd is a new RespMessage
                                    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new())));
                                } else {
                                    // put current resp back to the stack
                                    cmd_stack.push_back(resp);
                                }

                                cmd.clear();
                                maybe_split = false;
                            }
                        } else {
                            cmd.push(c);
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
