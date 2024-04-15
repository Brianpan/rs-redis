pub mod commands;
pub mod connection;
mod handler;
pub mod parser;

use hex;

// const CRLR: &str = "\r\n";

const RESP_OK: &str = "+OK\r\n";
const RESP_ERR: &str = "-ERR\r\n";
const RESP_PONG: &str = "+PONG\r\n";
const RESP_EMPTY: &str = "*0\r\n";

// hardcoded lenth
const PING_LEN: usize = 14;
const REPL_GETACK_LEN: usize = 37;

// preset id of master node (40 chars long)
// it will be changed to a random value in the future
const MYID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[derive(PartialEq, Clone)]
pub enum RespParsingState {
    ParsingMeta,
    ParsingData,
    End,
}

#[derive(PartialEq, Clone, Debug)]
pub enum RespType {
    Unknown,
    Integer,
    Error,
    SimpleString,
    BulkString,
    Array,
}

#[allow(dead_code)]
#[derive(PartialEq, Clone, Debug)]
pub enum RespCommandType {
    Error,
    Get(String),
    Set(String, String),
    SetPx(String, String, u64),
    Ping,
    Replconf(String),
}

pub enum CommandHandlerResponse {
    Basic(Vec<Vec<u8>>),
    Psync {
        message: Vec<Vec<u8>>,
        host: String,
    },
    Replica {
        message: Vec<Vec<u8>>,
        cmd: String,
    },
    Wait {
        message: Vec<Vec<u8>>,
        wait_time: u64,
        wait_count: u64,
    },
}

#[derive(PartialEq, Clone)]
pub struct RespMessage {
    pub remote_addr: String,
    resp_type: RespType,
    state: RespParsingState,
    int_data: i64,
    str_data: String,
    vec_data: Vec<RespMessage>,
}

impl RespMessage {
    pub fn new(addr: String) -> Self {
        RespMessage {
            remote_addr: addr,
            resp_type: RespType::Unknown,
            state: RespParsingState::ParsingMeta,
            int_data: 0,
            str_data: String::new(),
            vec_data: Vec::new(),
        }
    }

    pub fn parse(&mut self, data: &str) {
        let iter = data.chars().peekable();
        for c in iter {
            match c {
                '+' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::SimpleString;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '-' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Error;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                ':' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Integer;
                        self.state = RespParsingState::ParsingData;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '$' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::BulkString;
                    } else {
                        self.str_data.push(c);
                    }
                }
                '*' => {
                    if self.state == RespParsingState::ParsingMeta {
                        self.resp_type = RespType::Array;
                    } else {
                        self.str_data.push(c);
                    }
                }
                _ => {
                    // parsing logic
                    match self.resp_type {
                        RespType::SimpleString | RespType::Error => {
                            if self.state == RespParsingState::ParsingData {
                                self.str_data.push(c);
                            }
                        }
                        RespType::Integer => {
                            if self.state == RespParsingState::ParsingData {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            }
                        }
                        RespType::BulkString => {
                            if self.state == RespParsingState::ParsingMeta {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            } else if self.state == RespParsingState::ParsingData
                                && self.int_data > 0
                            {
                                self.str_data.push(c);
                                self.int_data -= 1;
                            }
                        }
                        RespType::Array => {
                            if self.state == RespParsingState::ParsingMeta {
                                self.int_data = self.int_data * 10 + c.to_digit(10).unwrap() as i64;
                            } else if self.state == RespParsingState::ParsingData {
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if self.state == RespParsingState::ParsingData {
            self.state = RespParsingState::End;
        } else if self.state == RespParsingState::ParsingMeta {
            self.state = RespParsingState::ParsingData;
        }
    }
}

pub fn string_to_bulk_string(s: String) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn string_to_bulk_string_for_psync(s: String) -> String {
    let rdb_decode = hex::decode(s).unwrap();
    format!("${}\r\n", rdb_decode.len())
}

pub fn array_to_resp_array(vec: Vec<String>) -> String {
    let mut ret = String::new();
    ret.push_str(format!("*{}\r\n", vec.len()).as_str());

    for v in vec {
        ret.push_str(&string_to_bulk_string(v));
    }

    return ret;
}

pub fn count_resp_command_type_offset(resp_command_type: RespCommandType) -> usize {
    match resp_command_type {
        RespCommandType::Error => 0,
        RespCommandType::Get(k) => {
            // *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
            // 13 + 5
            let mut base = 18;
            base += k.len();
            base += format!("{}", k.len()).len();
            base
        }
        RespCommandType::Set(k, v) => {
            // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n
            // 13 + 5 + 5
            let mut base = 23;
            base += k.len() + v.len();
            base += format!("{}{}", k.len(), v.len()).len();
            base
        }
        RespCommandType::SetPx(k, v, ttl) => {
            // *5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n$2\r\nPX\r\n$3\r\n100\r\n
            // 13 + 5 + 5 + 5 + 8
            // 8 is $2\r\nPX\r\n
            let mut base = 36;
            base += k.len() + v.len();
            base += format!("{}{}", k.len(), v.len()).len();
            base += format!("{}", ttl).len();
            base
        }
        RespCommandType::Ping => PING_LEN,
        RespCommandType::Replconf(k) => match k.as_str() {
            "getack" => REPL_GETACK_LEN,
            _ => 0,
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_resp_message_parse() {
        let cmd = RespCommandType::Get(String::from("foo"));
        assert_eq!(count_resp_command_type_offset(cmd), 22);
    }
}
