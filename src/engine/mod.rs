pub mod commands;
pub mod connection;

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
#[derive(PartialEq, Clone)]
pub struct RespMessage {
    resp_type: RespType,
    state: RespParsingState,
    int_data: i64,
    str_data: String,
    vec_data: Vec<RespMessage>,
}

impl RespMessage {
    pub fn new() -> Self {
        RespMessage {
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
