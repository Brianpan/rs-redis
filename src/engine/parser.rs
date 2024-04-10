use super::{RespCommandType, RespParsingState, RespType};
use anyhow;

// we assume that the input is always a RESP array
pub fn command_parser(input: &str) -> anyhow::Result<Vec<RespCommandType>> {
    let mut iter = input.char_indices().peekable();
    let mut parsing_state = RespParsingState::ParsingMeta;

    let mut current_resp_type = RespType::Array;

    let mut cmd_vec = Vec::new();
    let mut cmd_number = 0;

    let mut resp_vec = Vec::new();
    while let Some((pos, c)) = iter.next() {
        match c {
            '*' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    current_resp_type = RespType::Array;
                }
            }
            '$' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    current_resp_type = RespType::BulkString;
                }
            }
            '+' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    let simple_string = iter
                        .by_ref()
                        .take_while(|(_pos, c)| *c != '\r')
                        .map(|(_pos, c)| c)
                        .collect();
                    // expect next is \r\n
                    cmd_vec = Vec::new();
                    cmd_vec.push(simple_string);
                    resp_vec.push(process_command_vec(cmd_vec));
                    cmd_vec = Vec::new();
                }
            }
            '0'..='9' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    let start = pos;
                    let mut end = pos;
                    while iter.next_if(|&(_pos, ch)| ch.is_numeric()).is_some() {
                        end += 1;
                    }
                    if end < input.len() {
                        end += 1;
                    }
                    let num = input[start..end].to_owned().parse::<usize>()?;

                    if iter.next_if_eq(&(end, '\r')).is_some()
                        && iter.next_if_eq(&(end + 1, '\n')).is_some()
                    {
                        if current_resp_type == RespType::Array {
                            cmd_number = num;
                        } else {
                            parsing_state = RespParsingState::ParsingData;
                            let str_data = iter
                                .by_ref()
                                .take(num)
                                .map(|(_pos, ch)| ch)
                                .collect::<String>();

                            cmd_vec.push(str_data);
                            cmd_number -= 1;

                            // new command
                            // include string case
                            if cmd_number <= 0 {
                                resp_vec.push(process_command_vec(cmd_vec));
                                cmd_vec = Vec::new();
                                parsing_state = RespParsingState::ParsingMeta;
                            }
                        }
                    } else {
                        return Err(anyhow::anyhow!("Invalid RESP command"));
                    }
                }
            }
            _ => {
                if c == '\r' {
                    if iter.next_if_eq(&(pos + 1, '\n')).is_some() {
                        parsing_state = RespParsingState::ParsingMeta;
                    }
                }
            }
        }
    }

    Ok(resp_vec)
}

fn process_command_vec(cmd_vec: Vec<String>) -> RespCommandType {
    if cmd_vec.len() == 0 {
        return RespCommandType::Error;
    }

    match cmd_vec[0].to_lowercase().as_str() {
        "ping" => RespCommandType::Ping,
        "set" => {
            if cmd_vec.len() < 3 {
                return RespCommandType::Error;
            }

            if cmd_vec.len() == 3 {
                return RespCommandType::Set(cmd_vec[1].clone(), cmd_vec[2].clone());
            }
            if cmd_vec.len() != 5 {
                return RespCommandType::Error;
            }
            if cmd_vec[3].to_lowercase() != "px" {
                return RespCommandType::Error;
            }
            if let Ok(ttl) = cmd_vec[4].parse::<u64>() {
                RespCommandType::SetPx(cmd_vec[1].clone(), cmd_vec[2].clone(), ttl)
            } else {
                return RespCommandType::Error;
            }
        }
        "get" => {
            if cmd_vec.len() != 2 {
                return RespCommandType::Error;
            }
            RespCommandType::Get(cmd_vec[1].clone())
        }
        // return replconf
        "replconf" => {
            if cmd_vec.len() < 3 {
                return RespCommandType::Error;
            }

            match cmd_vec[1].to_lowercase().as_str() {
                "getack" => RespCommandType::Replconf(String::from("getack")),
                _ => RespCommandType::Error,
            }
        }
        _ => RespCommandType::Error,
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    #[test]
    fn command_parser_test() {
        assert_eq!(
            command_parser("*1\r\n$4\r\nping\r\n").unwrap()[0],
            RespCommandType::Ping
        );
        assert_eq!(
            command_parser("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n").unwrap()[0],
            RespCommandType::Set("key".to_string(), "value".to_string())
        );
        assert_eq!(
            command_parser(
                "*5\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$3\r\n100\r\n"
            )
            .unwrap()[0],
            RespCommandType::SetPx("key".to_string(), "value".to_string(), 100)
        );
        assert_eq!(
            command_parser("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n").unwrap()[0],
            RespCommandType::Get("key".to_string())
        );
        assert_eq!(
            command_parser("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n").unwrap()
                [0],
            RespCommandType::Error
        );
    }

    #[test]
    fn command_parser_test2() {
        let mut vec1 = Vec::new();
        vec1.push(RespCommandType::Set("foo".to_string(), "123".to_string()));
        vec1.push(RespCommandType::Set("bar".to_string(), "456".to_string()));
        assert_eq!(
            command_parser("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n").unwrap(),
            vec1,
        );

        let mut vec2 = Vec::new();
        vec2.push(RespCommandType::Replconf(("getack").to_string()));
        assert_eq!(
            command_parser("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n").unwrap(),
            vec2,
        );
    }
}
