use super::{HandshakeState, MasterInfo, NodeInfo, ReplicaType, SlaveInfo};
use crate::engine::parser::command_parser;
use crate::engine::{array_to_resp_array, count_resp_command_type_offset, RespCommandType};
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use tokio::net::tcp::OwnedWriteHalf;
// use std::io::prelude::*;
use crate::rdb::RdbConf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync;

// https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs

// const FULLRESYNC: &str = "+FULLRESYNC";

pub struct StoreEngine {
    dict: RwLock<HashMap<String, String>>,
    // key: stream key, value id -> HashMap<field, value>
    pub stream_dict: RwLock<HashMap<String, BTreeMap<StreamID, HashMap<String, String>>>>,
    pub stream_last_key: RwLock<HashMap<String, StreamID>>,
    expiring_queue: RwLock<PriorityQueue<String, Reverse<u128>>>,
    node_info: RwLock<NodeInfo>,
    pub rdb_info: Mutex<RdbConf>,
    pub replica_info: RwLock<ReplicaType>,
    pub master_info: RwLock<MasterInfo>,
    pub slave_info: RwLock<SlaveInfo>,
    pub replicas: sync::RwLock<HashMap<String, Arc<sync::Mutex<OwnedWriteHalf>>>>,
}

impl StoreEngine {
    pub fn new() -> Self {
        StoreEngine {
            dict: RwLock::new(HashMap::new()),
            stream_dict: RwLock::new(HashMap::new()),
            stream_last_key: RwLock::new(HashMap::new()),
            rdb_info: Mutex::new(RdbConf::default()),
            expiring_queue: RwLock::new(PriorityQueue::new()),
            replica_info: RwLock::new(ReplicaType::Master),
            node_info: RwLock::new(NodeInfo::default()),
            master_info: RwLock::new(MasterInfo::default()),
            slave_info: RwLock::new(SlaveInfo::default()),
            replicas: sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn set_node_info(&self, port: String) {
        *self.node_info.write().unwrap() = NodeInfo { port };
    }

    pub fn set_replica(&self, host: String) {
        *self.replica_info.write().unwrap() = ReplicaType::Slave(host.clone());
        *self.slave_info.write().unwrap() = SlaveInfo {
            host: host.split(":").collect::<Vec<&str>>()[0].to_string(),
            port: host.split(":").collect::<Vec<&str>>()[1].to_string(),
            master_replid: "?".to_string(),
            slave_repl_offset: 0,
            slave_ping_count: 0,
            slave_ack_count: 0,
            handshake_state: HandshakeState::Ping,
        }
    }

    pub fn set_replica_as_master(&self) {
        *self.replica_info.write().unwrap() = ReplicaType::Master;
    }

    pub fn get_replica(&self) -> ReplicaType {
        self.replica_info.read().unwrap().clone()
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let d = self.dict.read().unwrap();
        // clone the value to have new string
        // map Option<&T> -> Option<T>
        d.get(key).cloned()
    }

    pub fn set(&self, key: String, value: String) {
        self.dict.write().unwrap().insert(key, value);
    }

    pub fn set_with_expire(&self, key: String, value: String, ttl: u128) {
        let sys_time = SystemTime::now();
        let expired_ms = sys_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + ttl;
        self.dict.write().unwrap().insert(key.clone(), value);
        self.expiring_queue
            .write()
            .unwrap()
            .push(key, Reverse(expired_ms));
    }

    pub fn set_with_expire_exact(&self, key: String, value: String, ttl: u128) {
        self.dict.write().unwrap().insert(key.clone(), value);
        self.expiring_queue.write().unwrap().push(key, Reverse(ttl));
    }

    pub fn get_keys(&self) -> Vec<String> {
        <HashMap<String, String> as Clone>::clone(&self.dict.read().unwrap())
            .into_keys()
            .collect()
    }

    pub async fn expired_reaper(&self) {
        let sleep_time = Duration::from_millis(3);
        loop {
            let sys_time = SystemTime::now();
            let current_ms = sys_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            loop {
                if self.expiring_queue.read().unwrap().is_empty() {
                    break;
                }

                if let Some(elem) = self.expiring_queue.read().unwrap().peek() {
                    if *elem.1 < Reverse(current_ms) {
                        break;
                    }
                }

                let key = self
                    .expiring_queue
                    .read()
                    .unwrap()
                    .peek()
                    .unwrap()
                    .0
                    .clone();
                self.dict.write().unwrap().remove(&key);
                self.expiring_queue.write().unwrap().pop();
            }
            tokio::time::sleep(sleep_time).await;
        }
    }

    pub async fn handshake_to_master(&self) -> anyhow::Result<()> {
        if let ReplicaType::Slave(master) = self.get_replica() {
            let mut stream = TcpStream::connect(master).await?;

            let (rx, tx) = stream.split();
            let mut reader = BufReader::new(rx);
            let mut writer = BufWriter::new(tx);

            let redis_port = self.node_info.read().unwrap().port.clone();

            // phase 1: send PING to master
            let ping_cmd = array_to_resp_array(vec!["PING".to_string()]);

            // phase 2-1: send REPLCONF listening-port
            let replconf_cmd = array_to_resp_array(vec![
                "REPLCONF".to_string(),
                "listening-port".to_string(),
                redis_port.clone(),
            ]);

            // pase 2-2: send REPLCONF capa psync2
            let replconf_capa_cmd = array_to_resp_array(vec![
                "REPLCONF".to_string(),
                "capa".to_string(),
                "psync2".to_string(),
            ]);

            // phase 3: send PSYNC
            let psync_cmd = array_to_resp_array(vec![
                "PSYNC".to_string(),
                self.slave_info.read().unwrap().master_replid.clone(),
                "-1".to_string(),
            ]);
            let mut buf = [0; 1024];
            writer.write(ping_cmd.as_bytes()).await?;
            writer.flush().await?;

            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let resp = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    if !resp.contains("+PONG") {
                        return Err(anyhow::anyhow!("Handshake PING failed"));
                    }
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e));
                }
            }

            writer.write(replconf_cmd.as_bytes()).await?;
            writer.flush().await?;
            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let resp = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    if !resp.contains("+OK") {
                        return Err(anyhow::anyhow!("Handshake REPLCONF listening-port failed"));
                    }
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e));
                }
            }

            writer.write(replconf_capa_cmd.as_bytes()).await?;
            writer.flush().await?;
            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let resp = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    if !resp.contains("+OK") {
                        return Err(anyhow::anyhow!("Handshake REPLCONF capa psync2 failed"));
                    }
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e));
                }
            }

            writer.write(psync_cmd.as_bytes()).await?;
            writer.flush().await?;

            // read the command
            loop {
                let Ok(buf_len) = reader.read(&mut buf).await else {
                    continue;
                };

                if buf_len == 0 {
                    break;
                }

                let Ok(cmds) = command_parser(&mut buf[..buf_len]) else {
                    continue;
                };

                // println!("cmds: {:?}", cmds);

                for cmd in cmds {
                    match cmd.clone() {
                        RespCommandType::Set(key, value) => {
                            self.set(key, value);
                        }
                        RespCommandType::SetPx(key, value, ttl) => {
                            self.set_with_expire(key, value, ttl.into());
                        }
                        // reply ack with offset to the master
                        RespCommandType::Replconf(key) => {
                            // println!("receive healthcheck from master");
                            if key == "getack" {
                                // send ack to master
                                let ack_offset = self.slave_info.read().unwrap().slave_repl_offset;

                                let ack_cmd = array_to_resp_array(vec![
                                    "REPLCONF".to_string(),
                                    "ACK".to_string(),
                                    format!("{}", ack_offset),
                                ]);
                                writer.write(ack_cmd.as_bytes()).await?;
                                writer.flush().await?;
                            }
                        }
                        _ => {}
                    }

                    // add offset to the slave
                    let cmd_offset = count_resp_command_type_offset(cmd) as u64;
                    self.slave_info.write().unwrap().slave_repl_offset += cmd_offset;
                }
            }
        }

        Ok(())
    }
}

impl Default for StoreEngine {
    fn default() -> Self {
        StoreEngine::new()
    }
}

#[derive(Clone, Eq, Hash, PartialEq, Debug, Default)]
pub struct StreamID {
    pub millisecond: u128,
    pub sequence: u64,
}

impl From<&str> for StreamID {
    fn from(s: &str) -> Self {
        let v: Vec<&str> = s.split("-").collect();

        StreamID {
            millisecond: v[0].parse::<u128>().unwrap(),
            sequence: v[1].parse::<u64>().unwrap(),
        }
    }
}

impl From<&StreamID> for String {
    fn from(s: &StreamID) -> Self {
        format!("{}-{}", s.millisecond, s.sequence)
    }
}

pub enum StreamIDState {
    Ok,
    MillisecondOnly(u128),
    GenerateSequence(u128), // store timestamp
    GenerateMillisecond,
    FirstStreamID(StreamID),
    LastStreamID,
    Err,
}

impl StreamID {
    pub fn new(ts: u128, seq: u64) -> Self {
        StreamID {
            millisecond: ts,
            sequence: seq,
        }
    }

    pub fn new_current_ts() -> StreamID {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        StreamID {
            millisecond: ts.as_millis(),
            sequence: 0,
        }
    }

    pub fn validate(s: &str) -> StreamIDState {
        // beginning of stream
        if s == "-" {
            return StreamIDState::FirstStreamID(StreamID::default());
        } else if s == "+" {
            return StreamIDState::LastStreamID;
        }

        let v = s.split("-").collect::<Vec<&str>>();
        if v.len() != 2 {
            if v[0] == "*" {
                return StreamIDState::GenerateMillisecond;
            }

            if let Ok(ts) = v[0].parse::<u128>() {
                return StreamIDState::MillisecondOnly(ts);
            }

            return StreamIDState::Err;
        }

        if v[0].parse::<u128>().is_err() {
            if v[0] == "*" {
                return StreamIDState::GenerateMillisecond;
            }
            return StreamIDState::Err;
        }

        if v[1].parse::<u64>().is_err() {
            if v[1] == "*" {
                let ts = v[0].parse::<u128>().unwrap();
                return StreamIDState::GenerateSequence(ts);
            }
            return StreamIDState::Err;
        }

        StreamIDState::Ok
    }

    pub fn next_sequence_id(&self) -> StreamID {
        StreamID {
            millisecond: self.millisecond,
            sequence: self.sequence + 1,
        }
    }
}

impl PartialOrd for StreamID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.millisecond == other.millisecond {
            return self.sequence.cmp(&other.sequence);
        }

        self.millisecond.cmp(&other.millisecond)
    }
}
