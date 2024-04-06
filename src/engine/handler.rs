use std::sync::{Arc, RwLock};

use super::{
    string_to_bulk_string, string_to_bulk_string_for_psync, CommandHandlerResponse, RespMessage,
    EMPTY_RDB, MYID, RESP_OK,
};

use crate::store::engine::StoreEngine;
use crate::store::master_engine::MasterEngine;
use crate::store::{HandshakeState, ReplicaType};

use anyhow::Result;
use std::net::TcpStream;

pub fn handle_info(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut lookup_keys: Vec<String> = Vec::new();
    for i in 1..cmd.read().unwrap().vec_data.len() {
        lookup_keys.push(cmd.read().unwrap().vec_data[i as usize].str_data.clone());
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

pub fn handle_psync(
    db: &Arc<StoreEngine>,
    stream: Arc<RwLock<TcpStream>>,
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

    // we need to store stream to replicas
    db.set_replicas(host.clone(), stream.clone());

    resp_vec.push(rdb_vec);
    Ok(CommandHandlerResponse::Basic(resp_vec))
}

pub fn handle_replica(
    db: &Arc<StoreEngine>,
    cmd: Arc<RwLock<RespMessage>>,
) -> Result<CommandHandlerResponse> {
    let mut resp_vec = Vec::new();

    if cmd.read().unwrap().vec_data.len() > 2 {
        let host = cmd.read().unwrap().remote_addr.clone();
        println!("replica remote host: {}", host.clone());

        match cmd.read().unwrap().vec_data[1]
            .str_data
            .to_lowercase()
            .as_str()
        {
            "listening-port" => {
                let stream_port = cmd.read().unwrap().vec_data[2].str_data.clone();
                db.set_replica_as_master();
                db.set_slave_node(host.clone(), stream_port.clone(), HandshakeState::Replconf);
            }
            "capa" => {
                db.set_slave_node(host.clone(), String::from(""), HandshakeState::ReplconfCapa)
            }
            _ => {}
        }
    }
    let ret = RESP_OK;
    resp_vec.push(ret.to_string().as_bytes().to_vec());

    Ok(CommandHandlerResponse::Basic(resp_vec))
}
