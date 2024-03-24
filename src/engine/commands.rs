use std::sync::{Arc, RwLock};

use crate::store::engine::StoreEngine;

use super::{RespMessage, RespType};

use anyhow::Result;

// command consts
const COMMAND_GET: &str = "get";
const COMMAND_SET: &str = "set";
const COMMAND_PING: &str = "ping";
const COMMAND_ECHO: &str = "echo";

const RESP_OK: &str = "+OK\r\n";
const RESP_ERR: &str = "-ERR\r\n";
const RESP_PONG: &str = "+PONG\r\n";
const RESP_EMPTY: &str = "*0\r\n";

pub fn command_handler(db: &Arc<StoreEngine>, cmd: Arc<RwLock<RespMessage>>) -> Result<String> {
    let ret = match cmd.read().unwrap().resp_type {
        RespType::SimpleString => Ok(RESP_OK.to_string()),
        RespType::Error => Ok(RESP_ERR.to_string()),
        RespType::Integer => Ok(format!(":{}\r\n", cmd.read().unwrap().int_data)),
        RespType::BulkString => {
            match cmd
                .read()
                .unwrap()
                .str_data
                .as_str()
                .to_lowercase()
                .as_str()
            {
                "ping" => Ok(RESP_PONG.to_string()),
                _ => Err(anyhow::anyhow!("Unknown command")),
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
                        "" => Ok(RESP_OK.to_string()),
                        COMMAND_GET => {
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();
                            match db.get(&key) {
                                Some(val) => Ok(format!("${}\r\n{}\r\n", val.len(), val)),
                                None => Ok("$-1\r\n".to_string()),
                            }
                        }
                        COMMAND_SET => {
                            let db = db.clone();
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();

                            // no value included
                            if cmd.read().unwrap().vec_data.len() < 3 {
                                return Ok(RESP_ERR.to_string());
                            }

                            let val = cmd.read().unwrap().vec_data[2].str_data.clone();
                            let cmd_len = cmd.read().unwrap().vec_data.len();
                            if cmd_len == 5
                                && cmd.read().unwrap().vec_data[3].str_data.to_lowercase() == "px"
                            {
                                let ttl = cmd.read().unwrap().vec_data[4]
                                    .str_data
                                    .parse::<u128>()
                                    .unwrap();
                                db.set_with_expire(key, val, ttl);
                            } else {
                                db.set(key, val);
                            }

                            Ok(RESP_OK.to_string())
                        }
                        COMMAND_PING => Ok(RESP_PONG.to_string()),
                        COMMAND_ECHO => {
                            let mut ret = String::new();
                            for i in 1..cmd.read().unwrap().vec_data.len() {
                                ret.push_str(&format!(
                                    "${}\r\n{}\r\n",
                                    cmd.read().unwrap().vec_data[i].str_data.len(),
                                    cmd.read().unwrap().vec_data[i].str_data
                                ));
                            }
                            Ok(ret)
                        }
                        _ => Ok(RESP_EMPTY.to_string()),
                    };
                }
                _ => return Ok(RESP_EMPTY.to_string()),
            }
        }
        _ => Err(anyhow::anyhow!("Unknown command")),
    };

    ret
}
