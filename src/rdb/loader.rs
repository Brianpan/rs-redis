use super::{length_encode_code, op_code};
use crate::store::engine::StoreEngine;
use anyhow::Result;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::str;

pub const RDB_MAGIC: &'static str = "REDIS";
enum RDBParseResult {
    Skip,
    Ok,
    Err,
}

#[derive(PartialEq)]
enum RDBParseType {
    None,
    Aux(HashMap<String, String>),
    DB(),
}

struct RDBParseState {
    parse_type: RDBParseType,
    is_finished: bool,
}

pub trait RDBLoader {
    fn load(&self, filename: String) -> Result<bool>;
    fn parse<R: Read>(&self, reader: &mut R) -> Result<bool>;
    fn verify_magic<R: Read>(&self, reader: &mut R) -> bool;
    fn verify_version<R: Read>(&self, reader: &mut R) -> bool;
    fn verify_aux<R: Read>(&self, reader: &mut R) -> RDBParseState;

    fn parse_length_encoding<R: Read>(&self, reader: &mut R) -> Result<(usize, bool)>;
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

        if !self.verify_version(reader) {
            return Err(anyhow::anyhow!("no version supported"));
        }

        let mut buf = [0; 1024];
        let mut parse_state = RDBParseState::default();

        loop {
            let next_op = reader.read_u8()?;
            match next_op {
                op_code::AUX => {
                    println!("aux");
                    let _ = self.verify_aux(reader);
                }
                op_code::EXPIRETIME => {
                    println!("expiretime");
                }
                op_code::EXPIRETIME_MS => {
                    println!("expiretime_ms");
                }
                op_code::RESIZEDB => {
                    println!("resizedb");
                }
                op_code::SELECTDB => {
                    println!("selectdb");
                }
                op_code::EOF => {
                    break;
                }
                0_u8..=249_u8 => {
                    println!("no such op code");
                }
            }
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

    fn verify_version<R: Read>(&self, reader: &mut R) -> bool {
        let mut buf = [0; 4];

        match reader.read(&mut buf) {
            Ok(4) => (),
            Ok(_) => return false,
            Err(_) => return false,
        }

        // println!(
        //     "{}",
        //     str::from_utf8(&buf)
        //         .unwrap()
        //         .to_string()
        //         .parse::<u32>()
        //         .unwrap()
        // );

        true
    }

    fn verify_aux<R: Read>(&self, reader: &mut R) -> RDBParseState {
        let aux = HashMap::new();

        RDBParseState {
            parse_type: RDBParseType::Aux(aux),
            is_finished: true,
        }
    }

    fn parse_length_encoding<R: Read>(&self, reader: &mut R) -> Result<(usize, bool)> {
        let enc_type = reader.read_u8()?;
        let length: usize;
        let mut is_encode = false;

        // take first 2 bits
        match (enc_type & 0xC0) >> 6 {
            length_encode_code::SIX_BITS => {
                length = (enc_type & 0x3F) as usize;
            }
            length_encode_code::FORTEEN_BITS => {
                let next_byte = reader.read_u8()?;
                length = (((enc_type & 0x3F) << 8) as usize) + next_byte as usize;
            }
            // least byte isn't the lowest
            length_encode_code::FOUR_BYTES => {
                let next_4_bytes = reader.read_u32::<BigEndian>()?;
                length = next_4_bytes as usize;
            }
            length_encode_code::ENCODED => {
                is_encode = true;
                length = (enc_type & 0x3F) as usize;
            }
            _ => return Err(anyhow::anyhow!("parse_length_encoding err")),
        }

        Ok((length, is_encode))
    }
}

impl Default for RDBParseState {
    fn default() -> Self {
        RDBParseState {
            parse_type: RDBParseType::None,
            is_finished: false,
        }
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

    #[test]
    fn test_one_key() {
        let file = "./files/one_key.rdb";
        let engine = StoreEngine::new();
        engine.load(file.to_owned());
    }
}
