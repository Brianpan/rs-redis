use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use crate::store::engine::StoreEngine;

use super::commands::command_handler;
use super::{RespMessage, RespParsingState, RespType};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;

pub async fn handle_connection(db: &Arc<StoreEngine>, mut stream: TcpStream) {
    let mut cmd = String::new();
    let mut buf = [0; 512];
    let mut maybe_split = false;

    // stack to handle nested commands
    let mut cmd_stack: VecDeque<Arc<RwLock<RespMessage>>> = VecDeque::new();

    // push the first element
    // this cmd stack is used to track nested commands specifically for array type
    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new())));

    loop {
        let _ = stream.readable().await;
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
                                // main function to parse the command
                                // the result is in RespMessage
                                resp.write().unwrap().parse(&cmd);
                                let sleep_time = Duration::from_millis(3);

                                if resp.read().unwrap().state == RespParsingState::End {
                                    // if the parent is an array, we need to check if it's done
                                    if let Some(parent) = cmd_stack.pop_back() {
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
                                            parent.write().unwrap().state = RespParsingState::End;
                                            if cmd_stack.is_empty() {
                                                let resp = command_handler(db, parent.clone());
                                                if let Ok(resps) = resp {
                                                    for resp in resps {
                                                        stream
                                                            .write_all(resp.as_bytes())
                                                            .await
                                                            .unwrap();
                                                        // tokio::time::sleep(sleep_time).await;
                                                    }

                                                    // stream
                                                    //     .write_all(resps.concat().as_bytes())
                                                    //     .await
                                                    //     .unwrap();
                                                }
                                            }
                                        } else {
                                            cmd_stack.push_back(parent);
                                        }
                                    } else if let Ok(resps) = command_handler(db, resp) {
                                        for resp in resps {
                                            stream.write_all(resp.as_bytes()).await.unwrap();
                                            // tokio::time::sleep(sleep_time).await;
                                        }
                                        // stream.write_all(resps.concat().as_bytes()).await.unwrap();
                                    }

                                    // next cmd is a new RespMessage
                                    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new())));
                                } else if resp.read().unwrap().resp_type == RespType::Array {
                                    cmd_stack.push_back(resp);
                                    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new())));
                                } else {
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
