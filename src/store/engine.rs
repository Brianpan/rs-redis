use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::TcpStream;
use std::sync::RwLock;
use std::time::*;

// https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs

const MYID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

#[derive(Clone)]
pub enum ReplicaType {
    Master,
    Slave(String),
}

pub struct StoreEngine {
    dict: RwLock<HashMap<String, String>>,
    expiring_queue: RwLock<PriorityQueue<String, Reverse<u128>>>,
    replica_info: RwLock<ReplicaType>,
    master_info: RwLock<MasterInfo>,
    slave_info: RwLock<SlaveInfo>,
}

pub struct MasterInfo {
    master_replid: String,
    master_repl_offset: u64,
}

pub struct SlaveInfo {
    host: String,
    port: String,
}

impl StoreEngine {
    pub fn new() -> Self {
        StoreEngine {
            dict: RwLock::new(HashMap::new()),
            expiring_queue: RwLock::new(PriorityQueue::new()),
            replica_info: RwLock::new(ReplicaType::Master),
            master_info: RwLock::new(MasterInfo::default()),
            slave_info: RwLock::new(SlaveInfo::default()),
        }
    }

    pub fn set_replica(&self, host: String) {
        *self.replica_info.write().unwrap() = ReplicaType::Slave(host.clone());
        *self.slave_info.write().unwrap() = SlaveInfo {
            host: host.split(":").collect::<Vec<&str>>()[0].to_string(),
            port: host.split(":").collect::<Vec<&str>>()[1].to_string(),
        }
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

    pub fn send_resp_to_master(&self, resp: String) -> anyhow::Result<()> {
        if let ReplicaType::Slave(master) = self.get_replica() {
            let mut stream = TcpStream::connect(master)?;
            stream.write(resp.as_bytes())?;
        }

        Ok(())
    }
}

impl Default for StoreEngine {
    fn default() -> Self {
        StoreEngine::new()
    }
}

impl Default for MasterInfo {
    fn default() -> Self {
        MasterInfo {
            master_replid: MYID.to_string(),
            master_repl_offset: 0,
        }
    }
}

impl Default for SlaveInfo {
    fn default() -> Self {
        SlaveInfo {
            host: String::new(),
            port: String::new(),
        }
    }
}
