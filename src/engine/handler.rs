use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::{
    array_to_resp_array, array_to_resp_array_for_xrange, array_to_simple_resp_array,
    count_resp_command_type_offset, string_error_simple_string, string_to_bulk_string,
    string_to_bulk_string_for_psync, string_to_simple_string, xrange_to_read_wrap,
    CommandHandlerResponse, RespCommandType, RespMessage, EMPTY_RDB, MYID, RESP_ERR, RESP_OK,
};

use crate::rdb::config::RDBConfigOps;
use crate::rdb::value_type_string;
use crate::store::engine::{StoreEngine, StreamID, StreamIDState};
use crate::store::master_engine::MasterEngine;
use crate::store::stream_engine::StreamEngine;
use crate::store::{HandshakeState, ReplicaType};

use anyhow::Result;

pub fn handle_info(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut lookup_keys: Vec<String> = Vec::new();
    for i in 1..cmd.read().unwrap().vec_data.len() {
        lookup_keys.push(cmd.read().unwrap().vec_data[i].str_data.clone());
    }

    let mut resp_vec = Vec::new();
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

    resp_vec.push(ret.as_bytes().to_vec());
    Ok(CommandHandlerResponse::Basic(resp_vec))
}

pub fn handle_set(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    // master node to memorize the offset from set command
    let offset;

    let key = cmd.read().unwrap().vec_data[1].str_data.clone();

    // no value included
    if cmd.read().unwrap().vec_data.len() < 3 {
        resp_vec.push(RESP_ERR.to_string().as_bytes().to_vec());
    }

    let val = cmd.read().unwrap().vec_data[2].str_data.clone();
    let cmd_len = cmd.read().unwrap().vec_data.len();

    let mut repl_command = format!("SET {} {}", key.clone(), val.clone());

    if cmd_len == 5 && cmd.read().unwrap().vec_data[3].str_data.to_lowercase() == "px" {
        let ttl = cmd.read().unwrap().vec_data[4]
            .str_data
            .parse::<u128>()
            .unwrap();
        db.set_with_expire(key.clone(), val.clone(), ttl);
        repl_command.push_str(format!(" {}", ttl.clone()).as_str());

        offset = count_resp_command_type_offset(RespCommandType::SetPx(
            key.clone(),
            val.clone(),
            ttl.try_into().unwrap(),
        ));
    } else {
        db.set(key.clone(), val.clone());
        offset = count_resp_command_type_offset(RespCommandType::Set(key.clone(), val.clone()));
    }

    resp_vec.push(RESP_OK.to_string().as_bytes().to_vec());

    // println!("set offset {}", offset);

    if db.should_sync_command() {
        Ok(CommandHandlerResponse::Replica {
            message: resp_vec,
            cmd: repl_command,
            offset: offset as u64,
        })
    } else {
        Ok(CommandHandlerResponse::Set {
            message: resp_vec,
            offset: offset as u64,
        })
    }
}

pub fn handle_psync(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let myid = db.get_master_id();

    // stage 1: return +FULLRESYNC and myid
    let ret = format!("+FULLRESYNC {} 0\r\n", myid);
    let mut resp_vec = Vec::new();
    resp_vec.push(ret.as_bytes().to_vec());
    let rdb_snapshot = hex::decode(EMPTY_RDB).unwrap();
    let mut rdb_vec: Vec<u8> = string_to_bulk_string_for_psync(EMPTY_RDB.to_string()).into();
    rdb_vec.extend(&rdb_snapshot);

    // update slave node handshake state
    let host = cmd.read().unwrap().remote_addr.clone();
    // no stream port needed
    db.set_slave_node(host.clone(), String::from(""), HandshakeState::Psync);

    resp_vec.push(rdb_vec);
    Ok(CommandHandlerResponse::Psync {
        message: resp_vec,
        host,
    })
}

pub fn handle_replica(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    let ret = RESP_OK;
    let mut is_getack = false;

    if cmd.read().unwrap().vec_data.len() > 2 {
        let host = cmd.read().unwrap().remote_addr.clone();

        match cmd.read().unwrap().vec_data[1]
            .str_data
            .to_lowercase()
            .as_str()
        {
            "listening-port" => {
                let stream_port = cmd.read().unwrap().vec_data[2].str_data.clone();
                db.set_replica_as_master();
                db.set_slave_node(host.clone(), stream_port.clone(), HandshakeState::Replconf);
                resp_vec.push(ret.to_string().as_bytes().to_vec());
            }
            "capa" => {
                db.set_slave_node(host.clone(), String::from(""), HandshakeState::ReplconfCapa);
                resp_vec.push(ret.to_string().as_bytes().to_vec());
            }
            "getack" => {
                let ack_cmd = array_to_resp_array(vec![
                    "REPLCONF".to_string(),
                    "GETACK".to_string(),
                    "*".to_string(),
                ]);
                resp_vec.push(ack_cmd.as_bytes().to_vec());
                is_getack = true;
            }
            "ack" => {
                if cmd.read().unwrap().vec_data.len() > 2 {
                    let offset = cmd.read().unwrap().vec_data[2]
                        .str_data
                        .clone()
                        .parse::<u64>()?;
                    // println!("{} ack offset {}", host.clone(), offset);
                    db.set_slave_offset(host.clone(), offset);
                    // resp_vec.push(ret.to_string().as_bytes().to_vec());
                }
            }
            _ => {}
        }
    }

    if is_getack {
        Ok(CommandHandlerResponse::GetAck(resp_vec))
    } else {
        Ok(CommandHandlerResponse::Basic(resp_vec))
    }
}

pub fn handle_wait(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    // let mut resp_vec = Vec::new();
    let mut timeout: u64 = 0;
    let mut count: u64 = 0;
    if cmd.read().unwrap().vec_data.len() > 2 {
        timeout = cmd.read().unwrap().vec_data[1]
            .str_data
            .clone()
            .parse::<u64>()
            .unwrap();
        count = cmd.read().unwrap().vec_data[2]
            .str_data
            .clone()
            .parse::<u64>()
            .unwrap();
    }

    let ret = format!(":{}\r\n", db.get_connected_replica_count());

    Ok(CommandHandlerResponse::Wait {
        _message: vec![ret.as_bytes().to_vec()],
        wait_count: count,
        wait_time: timeout,
    })
}

pub fn handle_config(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    if cmd.read().unwrap().vec_data.len() > 2 {
        if cmd.read().unwrap().vec_data[1]
            .str_data
            .to_lowercase()
            .as_str()
            != "get"
        {
            return Err(anyhow::anyhow!("unknown config command"));
        }

        match cmd.read().unwrap().vec_data[2]
            .str_data
            .to_lowercase()
            .as_str()
        {
            "dir" => {
                let dir_resp = array_to_resp_array(vec![String::from("dir"), db.get_dir()]);
                resp_vec.push(dir_resp.as_bytes().to_vec());
            }
            "dbfilename" => {
                let dbfilename_resp =
                    array_to_resp_array(vec![String::from("dbfilename"), db.get_filename()]);
                resp_vec.push(dbfilename_resp.as_bytes().to_vec());
            }
            _ => return Err(anyhow::anyhow!("unknown config command")),
        }
        Ok(CommandHandlerResponse::Basic(resp_vec))
    } else {
        Err(anyhow::anyhow!("command too short"))
    }
}

pub fn handle_keys(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    if cmd.read().unwrap().vec_data.len() > 1 {
        if cmd.read().unwrap().vec_data[1]
            .str_data
            .to_lowercase()
            .as_str()
            == "*"
        {
            resp_vec.push(array_to_resp_array(db.get_keys()).as_bytes().to_vec());
        }
        Ok(CommandHandlerResponse::Basic(resp_vec))
    } else {
        Err(anyhow::anyhow!("command too short"))
    }
}

pub(crate) fn handle_type(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    if cmd.read().unwrap().vec_data.len() > 1 {
        let key = &cmd.read().unwrap().vec_data[1].str_data;

        let type_str = match db.get(key.as_str()) {
            Some(_) => value_type_string::STRING,
            None => match db.get_stream_key(key) {
                Some(_) => value_type_string::STREAM,
                None => value_type_string::NONE,
            },
        };

        resp_vec.push(
            string_to_simple_string(type_str.to_string())
                .as_bytes()
                .to_vec(),
        );
        Ok(CommandHandlerResponse::Basic(resp_vec))
    } else {
        Err(anyhow::anyhow!("command too short"))
    }
}

static XDD_ID_ERROR: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";
static XDD_ID_ERROR_0: &str = "ERR The ID specified in XADD must be greater than 0-0";

pub(crate) fn handle_xadd(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();
    let cmd_len = cmd.read().unwrap().vec_data.len();
    if cmd_len > 3 {
        let key = &cmd.read().unwrap().vec_data[1].str_data;
        let id = &cmd.read().unwrap().vec_data[2].str_data;

        #[warn(unused_assignments)]
        let stream_id;

        match StreamID::validate(id) {
            StreamIDState::FirstStreamID(_) | StreamIDState::LastStreamID | StreamIDState::Err => {
                resp_vec.push(
                    string_error_simple_string(XDD_ID_ERROR.to_string())
                        .as_bytes()
                        .to_vec(),
                );
                return Ok(CommandHandlerResponse::Basic(resp_vec));
            }
            StreamIDState::MillisecondOnly(_ts) => {
                resp_vec.push(
                    string_error_simple_string(XDD_ID_ERROR.to_string())
                        .as_bytes()
                        .to_vec(),
                );
                return Ok(CommandHandlerResponse::Basic(resp_vec));
            }
            StreamIDState::GenerateSequence(ts) => {
                match db.next_stream_sequence_id(key.clone(), ts) {
                    Some(sid) => {
                        stream_id = sid;
                    }
                    None => {
                        resp_vec.push(
                            string_error_simple_string(XDD_ID_ERROR.to_string())
                                .as_bytes()
                                .to_vec(),
                        );
                        return Ok(CommandHandlerResponse::Basic(resp_vec));
                    }
                }
            }
            StreamIDState::GenerateMillisecond => {
                stream_id = StreamID::new_current_ts();
            }
            StreamIDState::Ok => {
                stream_id = StreamID::from(id.as_str());
            }
        }

        // post validation
        if stream_id == StreamID::default() {
            resp_vec.push(
                string_error_simple_string(XDD_ID_ERROR_0.to_string())
                    .as_bytes()
                    .to_vec(),
            );

            return Ok(CommandHandlerResponse::Basic(resp_vec));
        }

        // invalid stream id
        if !db.valid_stream_id(key.clone(), stream_id.clone()) {
            resp_vec.push(
                string_error_simple_string(XDD_ID_ERROR.to_string())
                    .as_bytes()
                    .to_vec(),
            );

            return Ok(CommandHandlerResponse::Basic(resp_vec));
        }

        let mut val_list = Vec::new();
        for i in 3..cmd_len {
            let val = &cmd.read().unwrap().vec_data[i].str_data;
            val_list.push(val.clone());
        }
        let val_len = val_list.len();

        if val_len == 0 || val_len % 2 != 0 {
            return Err(anyhow::anyhow!("key/value is not a pair"));
        }

        let mut hmap = HashMap::new();
        let mut idx = 0;
        let mut last_key = String::new();
        while idx < val_len {
            if idx % 2 == 0 {
                last_key = val_list[idx].to_string();
            } else {
                hmap.insert(last_key.clone(), val_list[idx].to_string());
            }
            idx += 1;
        }

        // insert the map to stream
        let resp = db.set_stream_key(key.clone(), stream_id, hmap)?;

        resp_vec.push(string_to_bulk_string(resp).as_bytes().to_vec());

        Ok(CommandHandlerResponse::Basic(resp_vec))
    } else {
        Err(anyhow::anyhow!("command too short"))
    }
}

pub(crate) fn handle_xrange(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();
    let cmd_len = cmd.read().unwrap().vec_data.len();
    if cmd_len < 4 {
        return Err(anyhow::anyhow!("command too short"));
    }

    let k = &cmd.read().unwrap().vec_data[1].str_data;
    let from = &cmd.read().unwrap().vec_data[2].str_data;
    let to = &cmd.read().unwrap().vec_data[3].str_data;
    let from_stream_key = match StreamID::validate(from) {
        StreamIDState::Ok => StreamID::from(from.clone().as_str()),
        StreamIDState::MillisecondOnly(ts) => StreamID::new(ts, 0),
        StreamIDState::FirstStreamID(sid) => sid,
        _ => return Err(anyhow::anyhow!("invalid stream id")),
    };

    let to_stream_key = match StreamID::validate(to) {
        StreamIDState::Ok => StreamID::from(to.clone().as_str()),
        StreamIDState::MillisecondOnly(ts) => StreamID::new(ts, 0),
        StreamIDState::LastStreamID => db.get_last_stream_id(k.clone()).unwrap_or_default(),
        _ => return Err(anyhow::anyhow!("invalid stream id")),
    };

    // println!("from {:?} to {:?}", from_stream_key, to_stream_key);

    let stream_range = db.get_stream_by_range(k.clone(), &from_stream_key, &to_stream_key);
    // println!("{:?}", array_to_resp_array_for_xrange(&stream_range));

    resp_vec.push(
        array_to_resp_array_for_xrange(&stream_range)
            .as_bytes()
            .to_vec(),
    );

    Ok(CommandHandlerResponse::Basic(resp_vec))
}

#[derive(PartialEq)]
enum XReadMode {
    Block,
    Stream,
    Streams,
}

pub(crate) fn handle_xread(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();
    let cmd_len = cmd.read().unwrap().vec_data.len();
    if cmd_len < 4 {
        return Err(anyhow::anyhow!("command too short"));
    }
    // block case which has several cases
    // 1. has data: return immediately
    // 2. no data: wait for period of time. if no data return nil

    // 1 is streams which allows return multiple streams
    let xread_mode;
    xread_mode = match cmd.read().unwrap().vec_data[1].str_data.as_str() {
        "streams" => XReadMode::Streams,
        "block" => XReadMode::Block,
        _ => XReadMode::Stream,
    };

    let mut waiting_time = 0;
    let mut start_key = 2;
    match xread_mode {
        XReadMode::Block => {
            waiting_time = match cmd.read().unwrap().vec_data[2].str_data.parse::<u64>() {
                Ok(t) => t,
                Err(_) => return Err(anyhow::anyhow!("invalid waiting time")),
            };
            // expect to have another streams string
            start_key = 4;
        }
        XReadMode::Streams => {
            start_key = 2;
        }
        _ => {}
    };

    if xread_mode == XReadMode::Block || xread_mode == XReadMode::Streams {
        let mut key_vec = Vec::new();
        let mut id_vec = Vec::new();

        for i in start_key..cmd_len {
            let v = &cmd.read().unwrap().vec_data[i].str_data;
            match StreamID::validate(v) {
                StreamIDState::Ok => {
                    id_vec.push(StreamID::from(v.clone().as_str()));
                }
                StreamIDState::MillisecondOnly(ts) => {
                    id_vec.push(StreamID::new(ts, 0));
                }
                StreamIDState::FirstStreamID(sid) => {
                    id_vec.push(sid);
                }
                _ => {
                    key_vec.push(v.clone());
                }
            }
        }

        // the key and id should be in pair
        if key_vec.len() != id_vec.len() {
            return Err(anyhow::anyhow!("key and id mismatch"));
        }
        // for block case
        // let actor handle this
        // special response type for handler
        if xread_mode == XReadMode::Block {
            return Ok(CommandHandlerResponse::StreamBlock {
                ms: waiting_time,
                key_vec,
                stream_id_vec: id_vec,
            });
        }

        // for streams case
        let xread_arr = db.get_xread_streams(key_vec, id_vec)?;
        resp_vec.push(array_to_simple_resp_array(xread_arr).as_bytes().to_vec());

        return Ok(CommandHandlerResponse::Basic(resp_vec));
    }

    // stream case
    let k = &cmd.read().unwrap().vec_data[2].str_data;
    let from = &cmd.read().unwrap().vec_data[3].str_data;

    let from_stream_key = match StreamID::validate(from) {
        StreamIDState::Ok => StreamID::from(from.clone().as_str()),
        StreamIDState::MillisecondOnly(ts) => StreamID::new(ts, 0),
        StreamIDState::FirstStreamID(sid) => sid,
        _ => return Err(anyhow::anyhow!("invalid stream id")),
    };

    // we need to pack one more layer of array for xread

    let stream_range = db.get_xread(k, &from_stream_key);
    let key_stream_wrap = xrange_to_read_wrap(
        k.as_str(),
        array_to_resp_array_for_xrange(&stream_range).as_str(),
    );
    let xadd_arr = vec![key_stream_wrap];
    resp_vec.push(array_to_simple_resp_array(xadd_arr).as_bytes().to_vec());

    Ok(CommandHandlerResponse::Basic(resp_vec))
}
