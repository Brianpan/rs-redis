use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::*;

// https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs

#[derive(Clone)]
pub enum ReplicaType {
    Master,
    Slave(String),
}

pub struct StoreEngine {
    dict: RwLock<HashMap<String, String>>,
    expiring_queue: RwLock<PriorityQueue<String, Reverse<u128>>>,
    replica_info: RwLock<ReplicaType>,
}

impl StoreEngine {
    pub fn new() -> Self {
        StoreEngine {
            dict: RwLock::new(HashMap::new()),
            expiring_queue: RwLock::new(PriorityQueue::new()),
            replica_info: RwLock::new(ReplicaType::Master),
        }
    }

    pub fn set_replica(&self, host: String) {
        *self.replica_info.write().unwrap() = ReplicaType::Slave(host);
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
        let sleep_time = Duration::from_millis(5);
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
}

impl Default for StoreEngine {
    fn default() -> Self {
        StoreEngine::new()
    }
}
