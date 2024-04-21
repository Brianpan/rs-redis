use crate::store::engine::StoreEngine;
use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str;

pub const RDB_MAGIC: &'static str = "REDIS";

pub trait RDBLoader {
    fn load(&self, filename: String) -> Result<bool>;
    fn parse<R: Read>(&self, reader: &mut R) -> Result<bool>;
    fn verify_magic<R: Read>(&self, reader: &mut R) -> bool;
}

impl RDBLoader for StoreEngine {
    fn load(&self, filename: String) -> Result<bool> {
        let file = File::open(filename)?;

        let mut reader = BufReader::new(file);

        self.parse(&mut reader)
    }

    fn parse<R: Read>(&self, reader: &mut R) -> Result<bool> {
        // chain of rules to parse the RDB file
        if !self.verify_magic(reader) {
            return Err(anyhow::anyhow!("wrong magic number"));
        }

        Ok(true)
    }

    fn verify_magic<R: Read>(&self, reader: &mut R) -> bool {
        let mut buf = [0; 5];

        match reader.read(&mut buf) {
            Ok(5) => (),
            Ok(_) => return false,
            Err(_) => return false,
        }
        // println!("{}", str::from_utf8(&buf).unwrap().to_string());
        if buf != RDB_MAGIC.as_bytes() {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::store::engine::StoreEngine;

    #[test]
    fn test_magic() {
        let file = "./files/empty_database.rdb";
        let engine = StoreEngine::new();

        assert_eq!(engine.load(file.to_owned()).unwrap_or(false), true);
    }
}
