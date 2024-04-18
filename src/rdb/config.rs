use crate::store::engine::StoreEngine;

pub trait RDBConfigOps {
    fn set_dir(&self, dir: String);
    fn set_filename(&self, filename: String);

    fn get_dir(&self) -> String;
    fn get_filename(&self) -> String;
}

impl RDBConfigOps for StoreEngine {
    fn set_dir(&self, dir: String) {
        (*self).rdb_info.lock().unwrap().dir = dir.clone();
    }

    fn set_filename(&self, filename: String) {
        (*self).rdb_info.lock().unwrap().filename = filename.clone();
    }

    fn get_dir(&self) -> String {
        (*self).rdb_info.lock().unwrap().dir.clone()
    }

    fn get_filename(&self) -> String {
        (*self).rdb_info.lock().unwrap().filename.clone()
    }
}
