use super::engine::StoreEngine;
use super::{HandshakeState, ReplicaType, SlaveInfo};
use crate::engine::array_to_resp_array;
// use std::io::prelude::*;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub trait MasterEngine {
    fn get_master_id(&self) -> String {
        return String::new();
    }
    fn is_master(&self) -> bool {
        return false;
    }
    fn set_slave_node(&self, _host: String, stream_port: String, _handshake_state: HandshakeState);
    fn get_slave_node(&self, host: String) -> Option<SlaveInfo>;

    fn should_sync_command(&self) -> bool;

    fn set_replicas(
        &self,
        host: String,
        stream: Arc<Mutex<OwnedWriteHalf>>,
    ) -> impl std::future::Future<Output = ()> + Send;
    fn sync_command(
        &self,
        cmd: String,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;

    fn healthcheck_to_slave(&self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;

    fn get_connected_replica_count(&self) -> u32;
}

impl MasterEngine for StoreEngine {
    fn get_master_id(&self) -> String {
        self.master_info.read().unwrap().master_replid.clone()
    }

    fn is_master(&self) -> bool {
        self.get_replica() == ReplicaType::Master
    }

    fn set_slave_node(&self, host: String, stream_port: String, handshake_state: HandshakeState) {
        let mut slave = SlaveInfo {
            host: host.clone(),
            port: stream_port,
            master_replid: self.get_master_id(),
            slave_repl_offset: 0,
            handshake_state,
        };

        match self
            .master_info
            .read()
            .unwrap()
            .slave_list
            .get(&host.clone())
        {
            Some(old_slave) => {
                slave.port = old_slave.port.clone();
                slave.slave_repl_offset = old_slave.slave_repl_offset;
            }
            None => {}
        }

        // to avoid deadlock
        self.master_info
            .write()
            .unwrap()
            .slave_list
            .insert(host.clone(), slave);
    }

    fn get_slave_node(&self, host: String) -> Option<SlaveInfo> {
        self.master_info
            .read()
            .unwrap()
            .slave_list
            .get(&host.clone())
            .cloned()
    }

    fn should_sync_command(&self) -> bool {
        self.is_master() && self.master_info.read().unwrap().slave_list.len() > 0
    }

    async fn set_replicas(&self, host: String, stream: Arc<Mutex<OwnedWriteHalf>>) {
        self.replicas
            .write()
            .await
            .insert(host.clone(), stream.clone());
    }

    async fn sync_command(&self, cmd: String) -> anyhow::Result<()> {
        if !self.should_sync_command() {
            return Err(anyhow::anyhow!("err: should not sync command"));
        }

        let _master_replid = self.get_master_id();
        let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

        let cmd_vec: Vec<String> = cmd.split_whitespace().map(|s| s.to_string()).collect();

        for (host, slave) in slave_list.iter_mut() {
            let cmd_vec1 = cmd_vec.clone();

            if slave.handshake_state == HandshakeState::Psync {
                // send command to slave
                let cmd = array_to_resp_array(cmd_vec1);
                if let Some(stream) = self.replicas.read().await.get(&host.clone()) {
                    let mut stream = stream.lock().await;
                    match stream.write_all(&cmd.as_bytes()).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(unreachable_code)]
    async fn healthcheck_to_slave(&self) -> anyhow::Result<()> {
        if !self.is_master() {
            return Err(anyhow::anyhow!("err: not master"));
        }
        let get_ack_cmd = array_to_resp_array(vec![
            "REPLCONF".to_string(),
            "GETACK".to_string(),
            "*".to_string(),
        ]);

        loop {
            let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

            for (host, slave) in slave_list.iter_mut() {
                if slave.handshake_state == HandshakeState::Psync {
                    // send command to slave
                    if let Some(stream) = self.replicas.read().await.get(&host.clone()) {
                        let mut stream = stream.lock().await;
                        match stream.write_all(&get_ack_cmd.as_bytes()).await {
                            Ok(_) => {
                                println!("sent healthcheck to slave: {}", host);
                            }
                            Err(e) => {
                                println!("err: {}", e);
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok(())
    }

    fn get_connected_replica_count(&self) -> u32 {
        self.master_info
            .read()
            .unwrap()
            .slave_list
            .iter()
            .map(|(_, v)| {
                if v.handshake_state == HandshakeState::Psync {
                    1
                } else {
                    0
                }
            })
            .sum()
    }
}
