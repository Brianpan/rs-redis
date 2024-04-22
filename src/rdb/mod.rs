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

pub mod op_code {
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
