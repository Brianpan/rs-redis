use super::engine::StoreEngine;
use super::{HandshakeState, ReplicaType, SlaveInfo};
use crate::engine::array_to_resp_array;
// use std::io::prelude::*;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
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
        stream: Arc<Mutex<TcpStream>>,
    ) -> impl std::future::Future<Output = ()> + Send;
    fn sync_command(
        &self,
        cmd: String,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
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

    async fn set_replicas(&self, host: String, stream: Arc<Mutex<TcpStream>>) {
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
                println!("syncing command to slave: {} {}", host, cmd.clone());
                // send command to slave
                let cmd = array_to_resp_array(cmd_vec1);
                if let Some(stream) = self.replicas.read().await.get(&host.clone()) {
                    println!("syncing command to slave2: {} {}", host, cmd.clone());
                    let mut stream = stream.lock().await;
                    let _ = stream.write_all(&cmd.as_bytes()).await;
                }

                // self.master_info
                //     .read()
                //     .unwrap()
                //     .replicas
                //     .get(&host.clone())
                //     .unwrap()
                //     .lock()
                //     .await
                //     .write_all(&cmd.as_bytes())
                //     .await;
                println!("TBD sync");
            }
        }
        Ok(())
    }
}
