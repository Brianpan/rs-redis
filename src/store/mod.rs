pub mod engine;
pub mod master_engine;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use std::net::TcpStream;

const MYID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

#[derive(Clone, PartialEq)]
pub enum HandshakeState {
    Ping,
    Replconf,
    ReplconfCapa,
    Psync,
}

#[derive(Clone, PartialEq)]
pub enum ReplicaType {
    Master,
    Slave(String),
}

pub struct NodeInfo {
    port: String,
}

pub struct MasterInfo {
    master_replid: String,
    master_repl_offset: u64,
    pub handshake_state: HandshakeState,
    slave_list: HashMap<String, SlaveInfo>,
    replicas: HashMap<String, Arc<RwLock<TcpStream>>>,
}

#[derive(Clone)]
pub struct SlaveInfo {
    host: String,
    pub port: String,
    master_replid: String,
    slave_repl_offset: u64,
    pub handshake_state: HandshakeState,
}

impl Default for NodeInfo {
    fn default() -> Self {
        NodeInfo {
            port: "6379".to_string(),
        }
    }
}

impl Default for MasterInfo {
    fn default() -> Self {
        MasterInfo {
            master_replid: MYID.to_string(),
            master_repl_offset: 0,
            handshake_state: HandshakeState::Ping,
            slave_list: HashMap::new(),
            replicas: HashMap::new(),
        }
    }
}

impl Default for SlaveInfo {
    fn default() -> Self {
        SlaveInfo {
            host: String::new(),
            port: String::new(),
            master_replid: "?".to_string(),
            slave_repl_offset: 0,
            handshake_state: HandshakeState::Ping,
        }
    }
}
