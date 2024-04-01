use std::sync::{Arc, RwLock};

use crate::store::engine::{ReplicaType, StoreEngine};

use super::{RespMessage, RespType};

use anyhow::Result;

// command consts
const COMMAND_GET: &str = "get";
const COMMAND_SET: &str = "set";
const COMMAND_PING: &str = "ping";
const COMMAND_ECHO: &str = "echo";
const COMMAND_INFO: &str = "info";
const COMMAND_REPLCONF: &str = "replconf";
const COMMAND_PSYNC: &str = "psync";

const RESP_OK: &str = "+OK\r\n";
const RESP_ERR: &str = "-ERR\r\n";
const RESP_PONG: &str = "+PONG\r\n";
const RESP_EMPTY: &str = "*0\r\n";

// preset id of master node (40 chars long)
// it will be changed to a random value in the future
const MYID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const EMPTY_RDB: &'static [u8; 176] = b"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

// we support multiple responses to handle commands like psync
pub fn command_handler(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<Vec<String>> {
    let mut resp_vec = Vec::new();
    let ret = match cmd.read().unwrap().resp_type {
        RespType::SimpleString => {
            resp_vec.push(RESP_OK.to_string());
            Ok(resp_vec)
        }
        RespType::Error => {
            resp_vec.push(RESP_ERR.to_string());
            Ok(resp_vec)
        }
        RespType::Integer => {
            resp_vec.push(format!(":{}\r\n", cmd.read().unwrap().int_data));
            Ok(resp_vec)
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
                    resp_vec.push(RESP_PONG.to_string());
                    Ok(resp_vec)
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
                            resp_vec.push(RESP_OK.to_string());
                            Ok(resp_vec)
                        }
                        COMMAND_GET => {
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();
                            match db.get(&key) {
                                Some(val) => {
                                    resp_vec.push(string_to_bulk_string(val));
                                }
                                None => {
                                    resp_vec.push("$-1\r\n".to_string());
                                }
                            }
                            Ok(resp_vec)
                        }
                        COMMAND_SET => {
                            let db = db.clone();
                            let key = cmd.read().unwrap().vec_data[1].str_data.clone();

                            // no value included
                            if cmd.read().unwrap().vec_data.len() < 3 {
                                resp_vec.push(RESP_ERR.to_string());
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

                            resp_vec.push(RESP_OK.to_string());
                            Ok(resp_vec)
                        }
                        COMMAND_PING => {
                            resp_vec.push(RESP_PONG.to_string());
                            Ok(resp_vec)
                        }
                        COMMAND_ECHO => {
                            let mut ret = String::new();
                            for i in 1..cmd.read().unwrap().vec_data.len() {
                                ret.push_str(&string_to_bulk_string(
                                    cmd.read().unwrap().vec_data[i].str_data.clone(),
                                ));
                            }
                            resp_vec.push(ret);
                            Ok(resp_vec)
                        }
                        COMMAND_INFO => handle_info(&db.clone(), cmd.clone()),
                        // replconf always return OK
                        COMMAND_REPLCONF => {
                            let ret = RESP_OK;
                            resp_vec.push(ret.to_string());
                            Ok(resp_vec)
                        }
                        // psync return from master node with fullresync and myid
                        COMMAND_PSYNC => handle_psync(&db.clone(), cmd.clone()),
                        _ => {
                            resp_vec.push(RESP_EMPTY.to_string());
                            Ok(resp_vec)
                        }
                    };
                }
                _ => {
                    resp_vec.push(RESP_EMPTY.to_string());
                    Ok(resp_vec)
                }
            }
        }
        _ => return Err(anyhow::anyhow!("Unknown command")),
    };

    ret
}

fn handle_info(db: &Arc<StoreEngine>, cmd: Arc<RwLock<RespMessage>>) -> Result<Vec<String>> {
    let mut lookup_keys: Vec<String> = Vec::new();
    for i in 1..cmd.read().unwrap().vec_data.len() {
        lookup_keys.push(cmd.read().unwrap().vec_data[i as usize].str_data.clone());
    }

    let mut ret_vec = Vec::new();
    let mut ret = String::new();

    if lookup_keys.is_empty() {
        let db_info = "db_size: 0".to_string();
        ret.push_str(&string_to_bulk_string(db_info));
    } else {
        let mut key_iter = lookup_keys.iter();
        let mut idx = 0;
        while let Some(k) = key_iter.next() {
            match k.to_lowercase().as_str() {
                "replication" => {
                    if idx == 0 {
                        // generate role info
                        match db.get_replica() {
                            ReplicaType::Master => {
                                let mut master_info = String::from("role:master\r\n");
                                let master_repl_id = format!("master_replid:{}\r\n", MYID);

                                // generate master_repl_id, master_repl_offset
                                master_info = master_info + &master_repl_id;
                                let master_repl_offset = "master_repl_offset:0".to_string();
                                master_info = master_info + &master_repl_offset;

                                ret.push_str(&string_to_bulk_string(master_info));
                            }
                            ReplicaType::Slave(_) => {
                                ret.push_str(&string_to_bulk_string("role:slave".to_string()));
                            }
                        }
                    }
                }
                _ => {}
            }
            idx += 1;
        }
    }

    ret_vec.push(ret);
    Ok(ret_vec)
}

fn handle_psync(db: &Arc<StoreEngine>, cmd: Arc<RwLock<RespMessage>>) -> Result<Vec<String>> {
    let myid = db.get_master_id();

    // stage 1: return +FULLRESYNC and myid
    let ret = format!("+FULLRESYNC {} 0\r\n", myid);
    let mut ret_vec = Vec::new();
    ret_vec.push(ret);

    // stage 2: return the RDB file
    ret_vec.push(string_to_bulk_string_for_psync(EMPTY_RDB));

    println!("rdb: {}", string_to_bulk_string_for_psync(EMPTY_RDB));
    Ok(ret_vec)
}

pub fn string_to_bulk_string(s: String) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn string_to_bulk_string_for_psync(s: &[u8]) -> String {
    // let byte_in_string = s
    //     .iter()
    //     .map(|&b| format!("{:08b}", b))
    //     .collect::<Vec<String>>()
    //     .join("");
    let byte_in_string = String::from_utf8_lossy(s);
    format!("${}\r\n{}", byte_in_string.len(), byte_in_string)
}

pub fn array_to_resp_array(vec: Vec<String>) -> String {
    let mut ret = String::new();
    ret.push_str(format!("*{}\r\n", vec.len()).as_str());

    for v in vec {
        ret.push_str(&string_to_bulk_string(v));
    }

    return ret;
}
