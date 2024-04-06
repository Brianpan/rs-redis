use std::sync::{Arc, RwLock};

use super::handler::{handle_info, handle_psync, handle_replica};
use super::{
    CommandHandlerResponse, RespMessage, RespType, RESP_EMPTY, RESP_ERR, RESP_OK, RESP_PONG,
};

use super::string_to_bulk_string;
use crate::store::engine::StoreEngine;
use crate::store::master_engine::MasterEngine;
use anyhow::Result;
use std::net::TcpStream;

// command consts
const COMMAND_GET: &str = "get";
const COMMAND_SET: &str = "set";
const COMMAND_PING: &str = "ping";
const COMMAND_ECHO: &str = "echo";
const COMMAND_INFO: &str = "info";
const COMMAND_REPLCONF: &str = "replconf";
const COMMAND_PSYNC: &str = "psync";

// we support multiple responses to handle commands like psync
pub fn command_handler(
    arc_stream: Arc<RwLock<TcpStream>>,
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();
    let ret = match cmd.read().unwrap().resp_type {
        RespType::SimpleString => {
            resp_vec.push(RESP_OK.to_string().as_bytes().to_vec());
            Ok(CommandHandlerResponse::Basic(resp_vec))
        }
        RespType::Error => {
            resp_vec.push(RESP_ERR.to_string().as_bytes().to_vec());
            Ok(CommandHandlerResponse::Basic(resp_vec))
        }
        RespType::Integer => {
            resp_vec.push(
                format!(":{}\r\n", cmd.read().unwrap().int_data)
                    .as_bytes()
                    .to_vec(),
            );
            Ok(CommandHandlerResponse::Basic(resp_vec))
        }
        RespType::BulkString => {
            match cmd
                .read()
                .unwrap()
                .str_data
                .as_str()
                .to_lowercase()
                .as_str()
            {
                "ping" => {
                    resp_vec.push(RESP_PONG.to_string().as_bytes().to_vec());
                    Ok(CommandHandlerResponse::Basic(resp_vec))
                }
                _ => return Err(anyhow::anyhow!("Unknown command")),
            }
        }
        RespType::Array => {
            // TODO
            println!("handling");

            match cmd.read().unwrap().vec_data[0].resp_type {
                RespType::BulkString => {
                    return match cmd.read().unwrap().vec_data[0]
                        .str_data
                        .to_lowercase()
                        .as_str()
                    {
                        "" => {
                            resp_vec.push(RESP_OK.to_string().as_bytes().to_vec());
                            Ok(CommandHandlerResponse::Basic(resp_vec))
                        }
                        COMMAND_GET => {
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();
                            match db.get(&key) {
                                Some(val) => {
                                    resp_vec.push(string_to_bulk_string(val).as_bytes().to_vec());
                                }
                                None => {
                                    resp_vec.push("$-1\r\n".to_string().as_bytes().to_vec());
                                }
                            }
                            Ok(CommandHandlerResponse::Basic(resp_vec))
                        }
                        COMMAND_SET => {
                            let db = db.clone();
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();

                            // no value included
                            if cmd.read().unwrap().vec_data.len() < 3 {
                                resp_vec.push(RESP_ERR.to_string().as_bytes().to_vec());
                            }

                            let val = cmd.read().unwrap().vec_data[2].str_data.clone();
                            let cmd_len = cmd.read().unwrap().vec_data.len();

                            let mut repl_command = format!("SET {} {}", key.clone(), val.clone());

                            if cmd_len == 5
                                && cmd.read().unwrap().vec_data[3].str_data.to_lowercase() == "px"
                            {
                                let ttl = cmd.read().unwrap().vec_data[4]
                                    .str_data
                                    .parse::<u128>()
                                    .unwrap();
                                db.set_with_expire(key.clone(), val.clone(), ttl);
                                repl_command.push_str(format!(" {}", ttl.clone()).as_str());
                            } else {
                                db.set(key.clone(), val.clone());
                            }

                            resp_vec.push(RESP_OK.to_string().as_bytes().to_vec());

                            if db.should_sync_command() {
                                Ok(CommandHandlerResponse::Replica {
                                    message: resp_vec,
                                    cmd: repl_command,
                                })
                            } else {
                                Ok(CommandHandlerResponse::Basic(resp_vec))
                            }
                        }
                        COMMAND_PING => {
                            println!("get ping");
                            resp_vec.push(RESP_PONG.to_string().as_bytes().to_vec());
                            Ok(CommandHandlerResponse::Basic(resp_vec))
                        }
                        COMMAND_ECHO => {
                            let mut ret = String::new();
                            for i in 1..cmd.read().unwrap().vec_data.len() {
                                ret.push_str(&string_to_bulk_string(
                                    cmd.read().unwrap().vec_data[i].str_data.clone(),
                                ));
                            }
                            resp_vec.push(ret.as_bytes().to_vec());
                            Ok(CommandHandlerResponse::Basic(resp_vec))
                        }
                        COMMAND_INFO => handle_info(&db.clone(), cmd.clone()),
                        // replconf always return OK
                        COMMAND_REPLCONF => handle_replica(&db.clone(), cmd.clone()),
                        // psync return from master node with fullresync and myid
                        COMMAND_PSYNC => handle_psync(&db.clone(), arc_stream.clone(), cmd.clone()),
                        _ => {
                            resp_vec.push(RESP_EMPTY.to_string().as_bytes().to_vec());
                            Ok(CommandHandlerResponse::Basic(resp_vec))
                        }
                    };
                }
                _ => {
                    resp_vec.push(RESP_EMPTY.to_string().as_bytes().to_vec());
                    Ok(CommandHandlerResponse::Basic(resp_vec))
                }
            }
        }
        _ => return Err(anyhow::anyhow!("Unknown command")),
    };

    ret
}
