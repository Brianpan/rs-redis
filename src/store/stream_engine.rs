use super::engine::{StoreEngine, StreamID};
use anyhow::*;
use std::collections::HashMap;

pub trait StreamEngine {
    fn set_stream_key(
        &self,
        k: impl AsRef<str>,
        id: StreamID,
        hash: HashMap<String, String>,
    ) -> Result<String>;

    fn get_stream_key(
        &self,
        k: impl AsRef<str>,
    ) -> Option<HashMap<StreamID, HashMap<String, String>>>;

    fn valid_stream_id(&self, k: impl AsRef<str>, id: StreamID) -> bool;

    fn next_stream_sequence_id(&self, k: impl AsRef<str>, ts: u128) -> Option<StreamID>;
}

impl StreamEngine for StoreEngine {
    fn set_stream_key(
        &self,
        k: impl AsRef<str>,
        id: StreamID,
        hash: HashMap<String, String>,
    ) -> Result<String> {
        let key = k.as_ref().to_string();
        let mut hmap = HashMap::new();
        if let Some(id_map) = self.stream_dict.read().unwrap().get(&key) {
            hmap = id_map.clone();
        }

        hmap.insert(id.clone(), hash);

        self.stream_dict.write().unwrap().insert(key.clone(), hmap);

        // we also require to update last stream id
        self.stream_last_key
            .write()
            .unwrap()
            .insert(key, id.clone());

        Ok((&id).into())
    }

    fn get_stream_key(
        &self,
        k: impl AsRef<str>,
    ) -> Option<HashMap<StreamID, HashMap<String, String>>> {
        let d = self.stream_dict.read().unwrap();
        d.get(k.as_ref()).clone().cloned()
    }

    fn valid_stream_id(&self, k: impl AsRef<str>, id: StreamID) -> bool {
        let key = k.as_ref().to_string();

        if let Some(sid) = self.stream_last_key.read().unwrap().get(&key) {
            if sid >= &id {
                return false;
            }
        }

        true
    }

    fn next_stream_sequence_id(&self, k: impl AsRef<str>, ts: u128) -> Option<StreamID> {
        let key = k.as_ref().to_string();

        if let Some(sid) = self.stream_last_key.read().unwrap().get(&key) {
            // not valid
            if sid.millisecond > ts {
                None
            } else if sid.millisecond < ts {
                Some(StreamID::new(ts, 0))
            } else {
                Some(sid.next_sequence_id())
            }
        } else {
            // edge case ts == 0
            if ts == 0 {
                Some(StreamID::new(ts, 1))
            } else {
                Some(StreamID::new(ts, 0))
            }
        }
    }
}
