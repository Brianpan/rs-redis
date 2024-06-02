use super::engine::StoreEngine;
use anyhow::*;
use std::collections::HashMap;

pub trait StreamEngine {
    fn set_stream_key(
        &self,
        k: impl AsRef<str>,
        id: impl AsRef<str>,
        hash: HashMap<String, String>,
    ) -> Result<String>;
    fn get_stream_key(
        &self,
        k: impl AsRef<str>,
    ) -> Option<HashMap<String, HashMap<String, String>>>;
}

impl StreamEngine for StoreEngine {
    fn set_stream_key(
        &self,
        k: impl AsRef<str>,
        id: impl AsRef<str>,
        hash: HashMap<String, String>,
    ) -> Result<String> {
        // return ID
        let key = k.as_ref().to_string();
        let id = id.as_ref().to_string();
        let mut hmap = HashMap::new();
        if let Some(id_map) = self.stream_dict.read().unwrap().get(&key) {
            hmap = id_map.clone();
        }
        hmap.insert(id.clone(), hash);

        self.stream_dict.write().unwrap().insert(key, hmap);
        Ok(id)
    }

    fn get_stream_key(
        &self,
        k: impl AsRef<str>,
    ) -> Option<HashMap<String, HashMap<String, String>>> {
        let d = self.stream_dict.read().unwrap();
        d.get(k.as_ref()).clone().cloned()
    }
}
