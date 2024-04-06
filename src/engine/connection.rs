use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use super::commands::command_handler;
use super::{RespMessage, RespParsingState, RespType};
use crate::engine::CommandHandlerResponse;
use crate::store::engine::StoreEngine;
use crate::store::replicator::ReplicatorHandle;

use std::io::prelude::*;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub async fn handle_connection(
    db: &Arc<StoreEngine>,
    stream: Arc<Mutex<TcpStream>>,
    addr: SocketAddr,
) {
    let mut cmd = String::new();
    let mut buf = [0; 512];
    let mut maybe_split = false;

    let addr = addr.to_string();
    // println!("New connection from: {}", addr.clone());

    // stack to handle nested commands
    let mut cmd_stack: VecDeque<Arc<RwLock<RespMessage>>> = VecDeque::new();

    // push the first element
    // this cmd stack is used to track nested commands specifically for array type
    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new(addr.clone()))));

    let actor = ReplicatorHandle::new(db.clone());
    // tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    println!("New connection from: {}", addr.clone());
    loop {
        let _ = stream.lock().await.readable();
        let chrs = stream.lock().await.read(&mut buf).await;
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
                                                let resp = command_handler(
                                                    stream.clone(),
                                                    db,
                                                    parent.clone(),
                                                );
                                                if let Ok(resps) = resp {
                                                    command_handler_callback(
                                                        resps,
                                                        stream.clone(),
                                                        &actor,
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else {
                                            cmd_stack.push_back(parent);
                                        }
                                    } else if let Ok(resps) =
                                        command_handler(stream.clone(), db, resp)
                                    {
                                        command_handler_callback(resps, stream.clone(), &actor)
                                            .await;
                                    }

                                    // next cmd is a new RespMessage
                                    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new(
                                        addr.clone(),
                                    ))));
                                } else if resp.read().unwrap().resp_type == RespType::Array {
                                    cmd_stack.push_back(resp);
                                    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new(
                                        addr.clone(),
                                    ))));
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
                continue;
            }
        }
    }
}

async fn command_handler_callback(
    resps: CommandHandlerResponse,
    stream: Arc<Mutex<TcpStream>>,
    actor: &ReplicatorHandle,
) {
    match resps {
        CommandHandlerResponse::Basic(resps) => {
            for resp in resps {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::Replica { message, cmd } => {
            let _ = actor.set_op(cmd).await;
            for resp in message {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
    }
}
