use super::{HandshakeState, MasterInfo, NodeInfo, ReplicaType, SlaveInfo};
use crate::engine::array_to_resp_array;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::RwLock;
use std::time::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

// https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs

const FULLRESYNC: &str = "+FULLRESYNC";

pub struct StoreEngine {
    dict: RwLock<HashMap<String, String>>,
    expiring_queue: RwLock<PriorityQueue<String, Reverse<u128>>>,
    node_info: RwLock<NodeInfo>,
    pub replica_info: RwLock<ReplicaType>,
    pub master_info: RwLock<MasterInfo>,
    pub slave_info: RwLock<SlaveInfo>,
}

impl StoreEngine {
    pub fn new() -> Self {
        StoreEngine {
            dict: RwLock::new(HashMap::new()),
            expiring_queue: RwLock::new(PriorityQueue::new()),
            replica_info: RwLock::new(ReplicaType::Master),
            node_info: RwLock::new(NodeInfo::default()),
            master_info: RwLock::new(MasterInfo::default()),
            slave_info: RwLock::new(SlaveInfo::default()),
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
            println!("ping sent to master");

            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let resp = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    println!("ping response: {}", resp);
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
            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let resp = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    if !resp.contains(FULLRESYNC) {
                        return Err(anyhow::anyhow!("Handshake PSYNC failed"));
                    }
                    // parse the master id
                    // handmade parsing this time,
                    // will use nom parser to write RESP protocol parser in the future
                    // +FULLRESYNC <REPL_ID> 0\r\n
                    // simple string format so it's easier for us to parse
                    let mut next_is_masterid = false;
                    while let Some(word) = resp.split_whitespace().next() {
                        if word == FULLRESYNC {
                            next_is_masterid = true;
                            continue;
                        } else if next_is_masterid {
                            (*self.slave_info.write().unwrap()).master_replid =
                                word.to_string().clone();

                            break;
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e));
                }
            }
            // stream.read(&mut buf)?;

            // read rdb file
            match reader.read(&mut buf).await {
                Ok(buf_len) => {
                    let rdb = String::from_utf8_lossy(buf[..buf_len].as_ref());
                    println!("rdb: {}", rdb);
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e));
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
