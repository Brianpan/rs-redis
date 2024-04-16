use super::commands::command_handler;
use super::{RespMessage, RespParsingState, RespType};
use crate::engine::CommandHandlerResponse;
use crate::store::engine::StoreEngine;
use crate::store::master_engine::MasterEngine;
use crate::store::replicator::ReplicatorHandle;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub async fn handle_connection(db: &Arc<StoreEngine>, stream: TcpStream, addr: SocketAddr) {
    let mut cmd = String::new();
    let mut buf = [0; 512];
    let mut maybe_split = false;

    let addr = addr.to_string();

    // stack to handle nested commands
    let mut cmd_stack: VecDeque<Arc<RwLock<RespMessage>>> = VecDeque::new();

    // push the first element
    // this cmd stack is used to track nested commands specifically for array type
    cmd_stack.push_back(Arc::new(RwLock::new(RespMessage::new(addr.clone()))));

    let (mut rx, tx) = stream.into_split();
    let arc_tx = Arc::new(Mutex::new(tx));

    let actor = ReplicatorHandle::new(db.clone());

    loop {
        let _ = rx.readable();
        let chrs = rx.read(&mut buf).await;
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
                                                let resp = command_handler(db, parent.clone());
                                                if let Ok(resps) = resp {
                                                    command_handler_callback(
                                                        db.clone(),
                                                        resps,
                                                        &arc_tx.clone(),
                                                        &actor,
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else {
                                            cmd_stack.push_back(parent);
                                        }
                                    } else if let Ok(resps) = command_handler(db, resp) {
                                        command_handler_callback(
                                            db.clone(),
                                            resps,
                                            &arc_tx.clone(),
                                            &actor,
                                        )
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
    db: Arc<StoreEngine>,
    resps: CommandHandlerResponse,
    stream: &Arc<Mutex<OwnedWriteHalf>>,
    actor: &ReplicatorHandle,
) {
    match resps {
        CommandHandlerResponse::Basic(resps) => {
            for resp in resps {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::Set { message, offset } => {
            db.add_master_offset(offset);

            for resp in message {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::Psync { message, host } => {
            // we need to store stream to replicas
            db.set_replicas(host, stream.clone()).await;

            for resp in message {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::Replica {
            message,
            cmd,
            offset,
        } => {
            db.add_master_offset(offset);
            // println!(
            //     "add_master_offset master offset: {}",
            //     db.get_last_set_offset()
            // );
            let _ = actor.set_op(cmd).await;
            for resp in message {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::GetAck(resps) => {
            let _ = actor.getack_op().await;
            for resp in resps {
                stream.lock().await.write_all(&resp).await.unwrap();
            }
        }
        CommandHandlerResponse::Wait {
            _message,
            wait_count,
            wait_time,
        } => {
            let replicator_follow_count = actor.wait_op(wait_count, wait_time).await;
            let ret = format!(":{}\r\n", replicator_follow_count);
            stream
                .lock()
                .await
                .write_all(&ret.as_bytes())
                .await
                .unwrap();
        }
    }
}
