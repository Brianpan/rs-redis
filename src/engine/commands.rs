use std::sync::{Arc, RwLock};

use super::handler::{
    handle_config, handle_info, handle_keys, handle_psync, handle_replica, handle_set, handle_type,
    handle_wait,
};
use super::{
    CommandHandlerResponse, RespMessage, RespType, RESP_EMPTY, RESP_ERR, RESP_OK, RESP_PONG,
};

use super::string_to_bulk_string;
use crate::store::engine::StoreEngine;
use anyhow::Result;

// command consts
const COMMAND_GET: &str = "get";
const COMMAND_SET: &str = "set";
const COMMAND_PING: &str = "ping";
const COMMAND_ECHO: &str = "echo";
const COMMAND_INFO: &str = "info";
const COMMAND_REPLCONF: &str = "replconf";
const COMMAND_PSYNC: &str = "psync";
const COMMAND_WAIT: &str = "wait";
const COMMAND_CONFIG: &str = "config";
const COMMAND_KEYS: &str = "keys";
const COMMAND_TYPE: &str = "type";

// we support multiple responses to handle commands like psync
pub fn command_handler(
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
                        COMMAND_SET => handle_set(&db.clone(), cmd.clone()),
                        COMMAND_PING => {
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
                        COMMAND_WAIT => handle_wait(&db.clone(), cmd.clone()),
                        COMMAND_INFO => handle_info(&db.clone(), cmd.clone()),
                        // replconf always return OK
                        COMMAND_REPLCONF => handle_replica(&db.clone(), cmd.clone()),
                        // psync return from master node with fullresync and myid
                        COMMAND_PSYNC => handle_psync(&db.clone(), cmd.clone()),
                        COMMAND_CONFIG => handle_config(&db.clone(), cmd.clone()),
                        COMMAND_KEYS => handle_keys(&db.clone(), cmd.clone()),
                        COMMAND_TYPE => handle_type(&db.clone(), cmd.clone()),
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
