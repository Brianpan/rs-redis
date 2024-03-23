use std::sync::{Arc, RwLock};

use super::{RespMessage, RespType};

use anyhow::Result;

pub fn command_handler(cmd: Arc<RwLock<RespMessage>>) -> Result<String> {
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
                        "get" => Ok("$5\r\nhello\r\n".to_string()),
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
