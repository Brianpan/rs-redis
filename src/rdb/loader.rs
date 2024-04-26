use super::{length_encode_code, op_code, value_type};
use crate::store::engine::StoreEngine;
use anyhow::Result;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str;

pub const RDB_MAGIC: &'static str = "REDIS";
// enum RDBParseResult {
//     Skip,
//     Ok,
//     Err,
// }

#[derive(PartialEq, Debug)]
pub enum RDBParseType {
    None,
    Aux(HashMap<String, String>),
    DB(u8), // db number
    ResizeDB((u32, u32)),
}

#[derive(Debug)]
pub struct RDBParseState {
    parse_type: RDBParseType,
    is_finished: bool,
}

pub trait RDBLoader {
    fn load(&self, filename: String) -> Result<bool>;
    fn parse<R: Read>(&self, reader: &mut R) -> Result<bool>;
    fn verify_magic<R: Read>(&self, reader: &mut R) -> bool;
    fn verify_version<R: Read>(&self, reader: &mut R) -> bool;
    fn verify_aux<R: Read>(&self, reader: &mut R) -> Result<RDBParseState>;
    fn verify_db_selector<R: Read>(&self, reader: &mut R) -> Result<RDBParseState>;
    fn verify_resize_db<R: Read>(&self, reader: &mut R) -> Result<RDBParseState>;

    fn parse_value_encoding<R: Read>(&self, reader: &mut R) -> Result<String>;
    fn parse_length_encoding<R: Read>(&self, reader: &mut R) -> Result<(u32, bool)>;
    fn parse_string_encoding<R: Read>(&self, reader: &mut R) -> Result<String>;
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

        let mut cur_hash_size = 0;
        let mut cur_expire_hash_size = 0;
        let mut curdb = 0;
        loop {
            let next_op = reader.read_u8()?;
            match next_op {
                op_code::AUX => {
                    println!("aux");
                    let aux = self.verify_aux(reader)?;
                    println!("aux: {:?}", aux);
                }
                op_code::EXPIRETIME => {
                    println!("expiretime");
                }
                op_code::EXPIRETIME_MS => {
                    println!("expiretime_ms");
                }
                op_code::RESIZEDB => {
                    let state = self.verify_resize_db(reader)?;
                    match state.parse_type {
                        RDBParseType::ResizeDB((l1, l2)) => {
                            cur_hash_size = l1;
                            cur_expire_hash_size = l2;
                        }
                        _ => {}
                    }
                    println!("resizedb {} {}", cur_hash_size, cur_expire_hash_size);
                }
                op_code::SELECTDB => {
                    let state = self.verify_db_selector(reader)?;
                    match state.parse_type {
                        RDBParseType::DB(num) => {
                            curdb = num;
                        }
                        _ => {}
                    }
                    println!("selectdb {}", curdb);
                }
                op_code::EOF => {
                    break;
                }
                // 0..14 are parsing key,value
                op_code::STRING => {
                    let key = self.parse_string_encoding(reader)?;
                    let value = self.parse_string_encoding(reader)?;
                    // println!("key: {}, {}", key, value);
                    // put k,v into  db
                    self.set(key, value);
                }
                1_u8..=14_u8 => {
                    // parsing type
                    println!("unsupported type");
                }
                15_u8..=249_u8 => {
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

    fn verify_aux<R: Read>(&self, reader: &mut R) -> Result<RDBParseState> {
        let mut aux = HashMap::new();

        let k = self.parse_string_encoding(reader)?;
        let v = self.parse_string_encoding(reader)?;

        aux.insert(k, v);

        Ok(RDBParseState {
            parse_type: RDBParseType::Aux(aux),
            is_finished: true,
        })
    }

    fn verify_db_selector<R: Read>(&self, reader: &mut R) -> Result<RDBParseState> {
        let db_num = reader.read_u8()?;

        Ok(RDBParseState {
            parse_type: RDBParseType::DB(db_num),
            is_finished: false,
        })
    }

    fn verify_resize_db<R: Read>(&self, reader: &mut R) -> Result<RDBParseState> {
        let hashtable_size = self.parse_length_encoding(reader)?;
        let expire_hashtable_size = self.parse_length_encoding(reader)?;

        Ok(RDBParseState {
            parse_type: RDBParseType::ResizeDB((hashtable_size.0, expire_hashtable_size.0)),
            is_finished: true,
        })
    }

    fn parse_length_encoding<R: Read>(&self, reader: &mut R) -> Result<(u32, bool)> {
        let enc_type = reader.read_u8()?;
        let length: u32;
        let mut is_encode = false;

        // take first 2 bits
        match (enc_type & 0xC0) >> 6 {
            length_encode_code::SIX_BITS => {
                length = (enc_type & 0x3F) as u32;
            }
            length_encode_code::FORTEEN_BITS => {
                let next_byte = reader.read_u8()?;
                length = (((enc_type & 0x3F) as u32) << 8) as u32 | next_byte as u32;
            }
            // least byte isn't the lowest
            length_encode_code::FOUR_BYTES => {
                let next_4_bytes = reader.read_u32::<BigEndian>()?;
                length = next_4_bytes;
            }
            length_encode_code::ENCODED => {
                is_encode = true;
                length = (enc_type & 0x3F) as u32;
            }
            _ => return Err(anyhow::anyhow!("parse_length_encoding err")),
        }

        Ok((length, is_encode))
    }

    fn parse_string_encoding<R: Read>(&self, reader: &mut R) -> Result<String> {
        let (length, encoding) = self.parse_length_encoding(reader)?;

        // encoding case
        if encoding {
            return match length {
                // 8 bits interger
                0 => {
                    let i = reader.read_u8()?;
                    Ok(format!("{}", i))
                }
                // 16 bits integer
                1 => {
                    let i = reader.read_u16::<LittleEndian>()?;
                    Ok(format!("{}", i))
                }
                // 32 bits integer
                2 => {
                    let i = reader.read_u32::<LittleEndian>()?;
                    Ok(format!("{}", i))
                }
                // compressed string
                3 => Ok(String::from("")),
                _ => {
                    return Err(anyhow::anyhow!("not suppoerted"));
                }
            };
        }

        let mut buf = [0; 1024];
        // take limits of the length
        let mut handle = reader.take(length as u64);
        let mut remain = length;
        let mut s = String::new();
        loop {
            if remain <= 0 {
                break;
            }

            match handle.read(&mut buf) {
                Ok(n) => {
                    remain -= n as u32;
                    let s1 = str::from_utf8(&buf[..n])?;
                    s = format!("{}{}", s, s1);
                }
                Err(_) => {
                    return Err(anyhow::anyhow!("error when parsing string"));
                }
            }
        }

        Ok(s)
    }

    fn parse_value_encoding<R: Read>(&self, reader: &mut R) -> Result<String> {
        let enc_type = reader.read_u8()?;

        match enc_type {
            value_type::STRING => self.parse_string_encoding(reader),
            _ => Err(anyhow::anyhow!("value type not supported yet")),
        }
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
        let _ = engine.load(file.to_owned());
    }
}
