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
