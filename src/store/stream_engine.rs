use super::engine::StoreEngine;
use anyhow::*;
use std::collections::HashMap;

pub trait StreamEngine {
    fn set_stream_key(&self, k: impl AsRef<str>, hash: HashMap<String, String>) -> Result<String>;
    fn get_stream_key(&self, k: impl AsRef<str>) -> Option<HashMap<String, String>>;
}

impl StreamEngine for StoreEngine {
    fn set_stream_key(&self, k: impl AsRef<str>, hash: HashMap<String, String>) -> Result<String> {
        // return ID
        let key = k.as_ref().to_string();
        self.stream_dict.write().unwrap().insert(key.clone(), hash);
        Ok(key)
    }

    fn get_stream_key(&self, k: impl AsRef<str>) -> Option<HashMap<String, String>> {
        let d = self.stream_dict.read().unwrap();
        d.get(k.as_ref()).clone().cloned()
    }
}
