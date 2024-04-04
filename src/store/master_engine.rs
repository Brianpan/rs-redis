use super::engine::StoreEngine;
use super::{HandshakeState, HostPort, ReplicaType, SlaveInfo};
use crate::engine::commands::array_to_resp_array;
use std::io::prelude::*;
use std::net::TcpStream;

pub trait MasterEngine {
    fn get_master_id(&self) -> String {
        return String::new();
    }
    fn is_master(&self) -> bool {
        return false;
    }
    fn set_slave_node(&self, _host: String, _port: String, _handshake_state: HandshakeState) {}
    fn get_slave_node(&self, _host: String, _port: String) -> Option<SlaveInfo> {
        return None;
    }
    fn should_sync_command(&self) -> bool;
    fn sync_command(&self, cmd: String) -> bool;
}

impl MasterEngine for StoreEngine {
    fn get_master_id(&self) -> String {
        self.master_info.read().unwrap().master_replid.clone()
    }

    fn is_master(&self) -> bool {
        self.get_replica() == ReplicaType::Master
    }

    fn set_slave_node(&self, host: String, port: String, handshake_state: HandshakeState) {
        let host_port = (host.clone(), String::from(""));

        let mut slave = SlaveInfo {
            host,
            port,
            master_replid: self.get_master_id(),
            slave_repl_offset: 0,
            handshake_state,
        };

        match self.master_info.read().unwrap().slave_list.get(&host_port) {
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
            .insert(host_port, slave);
    }

    fn get_slave_node(&self, host: String, port: String) -> Option<SlaveInfo> {
        let host_port = (host, String::from(""));
        self.master_info
            .read()
            .unwrap()
            .slave_list
            .get(&host_port)
            .cloned()
    }

    fn should_sync_command(&self) -> bool {
        self.is_master() && self.master_info.read().unwrap().slave_list.len() > 0
    }

    fn sync_command(&self, cmd: String) -> bool {
        if !self.should_sync_command() {
            return false;
        }

        let sync = true;
        let _master_replid = self.get_master_id();
        let mut slave_list = self.master_info.read().unwrap().slave_list.clone();

        let mut buf = [0; 1024];

        let cmd_vec: Vec<String> = cmd.split_whitespace().map(|s| s.to_string()).collect();
        for (host_port, slave) in slave_list.iter_mut() {
            let cmd_vec1 = cmd_vec.clone();
            let host = format!("{}:{}", host_port.0.clone(), slave.port.clone());
            println!("send command to slave: {} {}", host.clone(), cmd.clone());
            if slave.handshake_state == HandshakeState::Psync {
                // send command to slave
                println!("send command2 to slave: {} {}", host.clone(), cmd.clone());
                if let Ok(mut stream) = TcpStream::connect(host.clone()) {
                    println!("send command3 to slave: {} {}", host.clone(), cmd.clone());
                    if let Ok(_) = stream.write(array_to_resp_array(cmd_vec1).as_bytes()) {
                        println!("send command4 to slave: {} {}", host.clone(), cmd.clone());
                        stream.read(&mut buf);
                    }
                }
            }
        }

        sync
    }
}
