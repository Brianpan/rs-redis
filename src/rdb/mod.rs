pub mod config;
pub mod loader;

pub struct RdbConf {
    dir: String,
    filename: String,
}

impl Default for RdbConf {
    fn default() -> Self {
        RdbConf {
            dir: String::from("/tmp/redis-files"),
            filename: String::from("dump.rdb"),
        }
    }
}

#[allow(dead_code)]
pub mod value_type {
    pub const STRING: u8 = 0;
    pub const LIST: u8 = 1;
    pub const SET: u8 = 2;
    pub const SORTED_SET: u8 = 3;
    pub const HASH: u8 = 4;
    pub const ZIPMAP: u8 = 9;
    pub const ZIPLIST: u8 = 10;
    pub const INTSET: u8 = 11;
    pub const SORTED_SET_ZIPLIST: u8 = 12;
    pub const HASH_ZIPLIST: u8 = 13;
    pub const LIST_QUICKLIST: u8 = 14;
}

pub mod op_code {
    pub const STRING: u8 = 0;

    pub const AUX: u8 = 250;
    pub const RESIZEDB: u8 = 251;
    pub const EXPIRETIME_MS: u8 = 252;
    pub const EXPIRETIME: u8 = 253;
    pub const SELECTDB: u8 = 254;
    pub const EOF: u8 = 255;
}

pub mod length_encode_code {
    pub const SIX_BITS: u8 = 0;
    pub const FORTEEN_BITS: u8 = 1;
    pub const FOUR_BYTES: u8 = 2;
    pub const ENCODED: u8 = 3;
}
