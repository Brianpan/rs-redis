use std::sync::{Arc, RwLock};

use crate::store::engine::StoreEngine;

use super::{RespMessage, RespType};

use anyhow::Result;

pub fn command_handler(db: &Arc<StoreEngine>, cmd: Arc<RwLock<RespMessage>>) -> Result<String> {
    let ret = match cmd.read().unwrap().resp_type {
        RespType::SimpleString => Ok("+OK\r\n".to_string()),
        RespType::Error => Ok("-ERR\r\n".to_string()),
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
                "ping" => Ok("+PONG\r\n".to_string()),
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
                        "" => Ok("+OK\r\n".to_string()),
                        "get" => {
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();
                            match db.get(&key) {
                                Some(val) => Ok(format!("${}\r\n{}\r\n", val.len(), val)),
                                None => Ok("$-1\r\n".to_string()),
                            }
                        }
                        "set" => {
                            let db = db.clone();
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();
                            let val = cmd.read().unwrap().vec_data[2].str_data.clone();
                            db.set(key, val);

                            Ok("+Ok\r\n".to_string())
                        }
                        "ping" => Ok("+PONG\r\n".to_string()),
                        "echo" => {
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
                        _ => Ok("*0\r\n".to_string()),
                    }
                }
                _ => return Ok("*0\r\n".to_string()),
            }
            // Ok("*0\r\n".to_string())
        }
        _ => Err(anyhow::anyhow!("Unknown command")),
    };

    ret
}
