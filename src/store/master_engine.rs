use super::engine::StoreEngine;
use super::{HandshakeState, ReplicaType, SlaveInfo};
use crate::engine::{array_to_resp_array, PING_LEN, REPL_GETACK_LEN};
// use std::io::prelude::*;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::time::{interval, sleep, Duration};

pub trait MasterEngine {
    fn get_master_id(&self) -> String {
        return String::new();
    }
    fn is_master(&self) -> bool {
        return false;
    }

    fn add_master_offset(&self, offset: u64);
    fn get_master_offset(&self) -> u64;

    fn set_last_send_offset(&self, offset: u64);
    fn get_last_send_offset(&self) -> u64;

    fn set_slave_node(&self, _host: String, stream_port: String, _handshake_state: HandshakeState);
    fn get_slave_node(&self, host: String) -> Option<SlaveInfo>;
    fn set_slave_offset(&self, host: String, offset: u64);

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

    // to collect wait time and count
    fn set_last_set_offset(&self, offset: u64);
    fn get_last_set_offset(&self) -> u64;

    fn send_ack_to_slave(&self) -> impl std::future::Future<Output = ()> + Send;
    fn get_ack_to_slave(&self) -> Vec<u64>;

    fn check_replica_follow(&self) -> impl std::future::Future<Output = u32> + Send;
    fn wait_replica(
        &self,
        wait_count: u64,
        wait_time: u64,
    ) -> impl std::future::Future<Output = u32> + Send;
}

impl MasterEngine for StoreEngine {
    fn get_master_id(&self) -> String {
        self.master_info.read().unwrap().master_replid.clone()
    }

    fn is_master(&self) -> bool {
        self.get_replica() == ReplicaType::Master
    }

    // offset operations for set only
    fn add_master_offset(&self, offset: u64) {
        if !self.is_master() {
            return;
        }
        self.master_info.write().unwrap().master_repl_offset += offset;
        let master_offset = self.get_master_offset();

        // we update the last set offset of the master offset
        self.set_last_set_offset(master_offset);
    }
    fn get_master_offset(&self) -> u64 {
        if !self.is_master() {
            return 0;
        }
        self.master_info.read().unwrap().master_repl_offset
    }

    // set last set offset
    fn set_last_set_offset(&self, offset: u64) {
        if !self.is_master() {
            return;
        }
        self.master_info.write().unwrap().last_set_offset = offset;
    }
    fn get_last_set_offset(&self) -> u64 {
        if !self.is_master() {
            return 0;
        }
        self.master_info.read().unwrap().last_set_offset
    }

    fn set_last_send_offset(&self, offset: u64) {
        if !self.is_master() {
            return;
        }
        self.master_info.write().unwrap().last_send_repl_offset = offset;
    }
    fn get_last_send_offset(&self) -> u64 {
        if !self.is_master() {
            return 0;
        }
        self.master_info.read().unwrap().last_send_repl_offset
    }

    fn set_slave_node(&self, host: String, stream_port: String, handshake_state: HandshakeState) {
        let mut slave = SlaveInfo {
            host: host.clone(),
            port: stream_port,
            master_replid: self.get_master_id(),
            slave_repl_offset: 0,
            slave_ping_count: 0,
            slave_ack_count: 0,
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
                slave.slave_ping_count = old_slave.slave_ping_count;
                slave.slave_ack_count = old_slave.slave_ack_count;
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
    fn set_slave_offset(&self, host: String, offset: u64) {
        let new_slave;
        if let Some(slave) = self
            .master_info
            .read()
            .unwrap()
            .slave_list
            .get(&host.clone())
        {
            let mut slave = slave.clone();
            slave.slave_repl_offset = offset;
            new_slave = slave;
        } else {
            return;
        }

        self.master_info
            .write()
            .unwrap()
            .slave_list
            .insert(host.clone(), new_slave.clone());

        println!("new slave offset: {} {}", host, new_slave.slave_repl_offset);
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
        // let get_ack_cmd = array_to_resp_array(vec![
        //     "REPLCONF".to_string(),
        //     "GETACK".to_string(),
        //     "*".to_string(),
        // ]);
        let ping_cmd = array_to_resp_array(vec!["PING".to_string()]);

        loop {
            let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

            for (host, slave) in slave_list.iter_mut() {
                if slave.handshake_state == HandshakeState::Psync {
                    // send command to slave
                    if let Some(stream) = self.replicas.read().await.get(&host.clone()) {
                        let mut stream = stream.lock().await;
                        match stream.write_all(&ping_cmd.as_bytes()).await {
                            Ok(_) => {
                                // println!("sent healthcheck to slave: {}", host);
                                slave.slave_ping_count += 1;
                            }
                            Err(e) => {
                                println!("err: {}", e);
                            }
                        }
                    }
                }
            }
            // [TBD] perhaps we shall update the ping count back

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

    async fn send_ack_to_slave(&self) {
        println!("send_ack_to_slave");

        let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

        for (host, slave) in slave_list.iter_mut() {
            if slave.handshake_state == HandshakeState::Psync {
                let get_ack_cmd = array_to_resp_array(vec![
                    "REPLCONF".to_string(),
                    "GETACK".to_string(),
                    "*".to_string(),
                ]);
                // send command to slave
                if let Some(stream) = self.replicas.read().await.get(&host.clone()) {
                    let mut stream = stream.lock().await;
                    match stream.write_all(&get_ack_cmd.as_bytes()).await {
                        Ok(_) => {
                            // here we need to wait for the ack from the slave
                            // println!(
                            //     "sent getack to slave: {} {}, {}",
                            //     host, offset, slave.slave_ack_count
                            // );

                            // we send one more ack to slave
                            slave.slave_ack_count += 1;
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
            }
        }
        let master_offset = self.get_master_offset();
        self.set_last_set_offset(master_offset);
    }

    fn get_ack_to_slave(&self) -> Vec<u64> {
        let mut ack_count = Vec::new();
        let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

        for (_host, slave) in slave_list.iter_mut() {
            if slave.handshake_state == HandshakeState::Psync {
                let mut offset = slave.slave_repl_offset;

                offset -= (slave.slave_ack_count - 1) * PING_LEN as u64
                    + slave.slave_ping_count * REPL_GETACK_LEN as u64;
                ack_count.push(offset);
            }
        }

        ack_count
    }

    async fn check_replica_follow(&self) -> u32 {
        let mut count = 0;
        let master_offset = self.get_master_offset();
        let last_send_offset = self.get_last_send_offset();
        // println!(
        //     "master_offset: {}, last_send_offset: {}",
        //     master_offset, last_send_offset
        // );

        // the master offset is updated. then, we need to send another ack to the slaves
        if master_offset != last_send_offset {
            self.send_ack_to_slave().await;
            self.set_last_send_offset(master_offset);
        }

        // iterate slave list to run replconf getack * to verify the replica's offset has reached the last set offset
        let slave_offsets = self.get_ack_to_slave();
        let last_set_offset = self.get_last_set_offset();
        // println!(
        //     "ack_list: {:?}, last_set_offset: {}",
        //     slave_offsets, last_set_offset
        // );

        for offset in slave_offsets.iter() {
            if offset >= &last_set_offset {
                count += 1;
            }
        }
        count
    }

    async fn wait_replica(&self, wait_count: u64, wait_time: u64) -> u32 {
        let mut count = 0;
        let mut ticker = interval(Duration::from_millis(20));
        // let timeout = sleep(Duration::from_millis(wait_time));
        // [TBD] offset counting

        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(wait_time)) => {
                    count = self.check_replica_follow().await;
                    break;
                },
                _ = async {
                    loop {
                        ticker.tick().await;
                        // execute check replica follow
                        count = self.check_replica_follow().await;
                        if u64::from(count) >= wait_count {
                            break;
                        }
                    }
                } => {
                    break;
                },
                else => {
                },
            };
        }
        count
    }
}
