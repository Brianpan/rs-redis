use super::engine::StoreEngine;
use super::{HandshakeState, HostPort, ReplicaType, SlaveInfo};

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
}

impl MasterEngine for StoreEngine {
    fn get_master_id(&self) -> String {
        self.master_info.read().unwrap().master_replid.clone()
    }

    fn is_master(&self) -> bool {
        self.get_replica() == ReplicaType::Master
    }

    fn set_slave_node(&self, host: String, port: String, handshake_state: HandshakeState) {
        let host_port = (host.clone(), port.clone());

        match self.master_info.read().unwrap().slave_list.get(&host_port) {
            Some(slave) => {
                let mut slave = slave.clone();
                slave.handshake_state = handshake_state;
                slave.slave_repl_offset = 0;
                slave.master_replid = self.get_master_id();
                self.master_info
                    .write()
                    .unwrap()
                    .slave_list
                    .insert(host_port, slave);
            }
            None => {
                let slave = SlaveInfo {
                    host,
                    port,
                    master_replid: self.get_master_id(),
                    slave_repl_offset: 0,
                    handshake_state,
                };
                self.master_info
                    .write()
                    .unwrap()
                    .slave_list
                    .insert(host_port, slave);
            }
        }
    }

    fn get_slave_node(&self, host: String, port: String) -> Option<SlaveInfo> {
        let host_port = (host, port);
        self.master_info
            .read()
            .unwrap()
            .slave_list
            .get(&host_port)
            .cloned()
    }
}
