pub mod engine;
pub mod master_engine;
pub mod replicator;
pub mod stream_engine;

use std::collections::HashMap;

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
#[allow(dead_code)]
pub struct MasterInfo {
    master_replid: String,
    master_repl_offset: u64,
    last_send_repl_offset: u64,
    last_set_offset: u64,
    pub handshake_state: HandshakeState,
    slave_list: HashMap<String, SlaveInfo>,
}
#[allow(dead_code)]
#[derive(Clone)]
pub struct SlaveInfo {
    host: String,
    pub port: String,
    master_replid: String,
    slave_repl_offset: u64,
    slave_ping_count: u64,
    slave_ack_count: u64,
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
            last_send_repl_offset: 0,
            last_set_offset: 0,
            handshake_state: HandshakeState::Ping,
            slave_list: HashMap::new(),
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
            slave_ping_count: 0,
            slave_ack_count: 0,
            handshake_state: HandshakeState::Ping,
        }
    }
}
