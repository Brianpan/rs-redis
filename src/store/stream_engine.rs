use super::engine::{StoreEngine, StreamID};
use crate::engine::{array_to_resp_array_for_xrange, xrange_to_read_wrap};
use anyhow::*;
use core::ops::Bound::{Excluded, Included};
use std::collections::{BTreeMap, HashMap};
#[derive(Debug)]
pub struct StreamRange {
    pub stream_id: StreamID,
    pub hash: HashMap<String, String>,
}

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
    ) -> Option<BTreeMap<StreamID, HashMap<String, String>>>;

    fn valid_stream_id(&self, k: impl AsRef<str>, id: StreamID) -> bool;

    fn get_last_stream_id(&self, k: impl AsRef<str>) -> Option<StreamID>;

    fn next_stream_sequence_id(&self, k: impl AsRef<str>, ts: u128) -> Option<StreamID>;

    fn get_stream_by_range(
        &self,
        k: impl AsRef<str>,
        start: &StreamID,
        end: &StreamID,
    ) -> Vec<StreamRange>;

    fn get_xread(&self, k: impl AsRef<str>, start: &StreamID) -> Vec<StreamRange>;
    fn get_xread_streams(
        &self,
        keys: Vec<String>,
        stream_ids: Vec<StreamID>,
    ) -> Result<Vec<String>>;
}

impl StreamEngine for StoreEngine {
    fn set_stream_key(
        &self,
        k: impl AsRef<str>,
        id: StreamID,
        hash: HashMap<String, String>,
    ) -> Result<String> {
        let key = k.as_ref().to_string();
        let mut hmap = BTreeMap::new();
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
    ) -> Option<BTreeMap<StreamID, HashMap<String, String>>> {
        let d = self.stream_dict.read().unwrap();
        d.get(k.as_ref()).clone().cloned()
    }

    fn valid_stream_id(&self, k: impl AsRef<str>, id: StreamID) -> bool {
        let key = k.as_ref().to_string();

        if let Some(sid) = self.get_last_stream_id(key) {
            if sid >= id {
                return false;
            }
        }

        true
    }

    fn get_last_stream_id(&self, k: impl AsRef<str>) -> Option<StreamID> {
        let key = k.as_ref().to_string();
        self.stream_last_key.read().unwrap().get(&key).cloned()
    }

    fn next_stream_sequence_id(&self, k: impl AsRef<str>, ts: u128) -> Option<StreamID> {
        let key = k.as_ref().to_string();

        if let Some(sid) = self.get_last_stream_id(key) {
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

    fn get_stream_by_range(
        &self,
        k: impl AsRef<str>,
        start: &StreamID,
        end: &StreamID,
    ) -> Vec<StreamRange> {
        let mut vec = Vec::new();
        let key = k.as_ref().to_string();

        if let Some(id_map) = self.stream_dict.read().unwrap().get(&key) {
            // empty check
            if let Some((k, _v)) = id_map.last_key_value() {
                if k < start {
                    return vec;
                }
            } else {
                return vec;
            }

            id_map
                .range((Included(start), Included(end)))
                .for_each(|(id, hash)| {
                    vec.push(StreamRange {
                        stream_id: id.clone(),
                        hash: hash.clone(),
                    })
                });
        }

        vec
    }

    fn get_xread(&self, k: impl AsRef<str>, start: &StreamID) -> Vec<StreamRange> {
        let mut vec = Vec::new();
        let key = k.as_ref().to_string();

        if let Some(id_map) = self.stream_dict.read().unwrap().get(&key) {
            // empty check
            if let Some((end, _v)) = id_map.last_key_value() {
                if end < start {
                    return vec;
                }

                id_map
                    .range((Excluded(start), Included(end)))
                    .for_each(|(id, hash)| {
                        vec.push(StreamRange {
                            stream_id: id.clone(),
                            hash: hash.clone(),
                        })
                    });
            } else {
                return vec;
            }
        }

        vec
    }

    fn get_xread_streams(
        &self,
        keys: Vec<String>,
        stream_ids: Vec<StreamID>,
    ) -> Result<Vec<String>> {
        let mut xread_arr = Vec::with_capacity(keys.len());

        for idx in 0..keys.len() {
            let key = keys[idx].clone();
            let from_stream_key = stream_ids[idx].clone();
            let stream_range = self.get_xread(key.clone(), &from_stream_key);
            if stream_range.is_empty() {
                continue;
            }

            let key_stream_wrap = xrange_to_read_wrap(
                key.as_str(),
                array_to_resp_array_for_xrange(&stream_range).as_str(),
            );
            xread_arr.push(key_stream_wrap);
        }

        Ok(xread_arr)
    }
}
