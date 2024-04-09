use super::{RespCommandType, RespParsingState, RespType};
use anyhow;

// we assume that the input is always a RESP array
pub fn command_parser(input: &str) -> anyhow::Result<RespCommandType> {
    let mut iter = input.char_indices().peekable();
    let mut parsing_state = RespParsingState::ParsingMeta;
    if iter.peek() != Some(&(0, '*')) {
        return Err(anyhow::anyhow!("Invalid RESP command"));
    }

    iter.next();
    let mut current_resp_type = RespType::Array;

    let mut cmd_vec = Vec::new();
    let mut cmd_number = 0;

    while let Some((pos, c)) = iter.next() {
        match c {
            '$' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    current_resp_type = RespType::BulkString;
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

    if cmd_vec.len() != cmd_number || cmd_number == 0 {
        return Err(anyhow::anyhow!("Invalid RESP command"));
    }

    println!("get cmd: {:?}", cmd_vec);

    match cmd_vec[0].to_lowercase().as_str() {
        "ping" => Ok(RespCommandType::Ping),
        "set" => {
            if cmd_vec.len() < 3 {
                return Err(anyhow::anyhow!("Invalid RESP command"));
            }

            if cmd_vec.len() == 3 {
                return Ok(RespCommandType::Set(cmd_vec[1].clone(), cmd_vec[2].clone()));
            }
            if cmd_vec.len() != 5 {
                return Err(anyhow::anyhow!("Invalid RESP command"));
            }
            if cmd_vec[3].to_lowercase() != "px" {
                return Err(anyhow::anyhow!("Invalid RESP command"));
            }
            let ttl = cmd_vec[4].parse::<u64>()?;
            Ok(RespCommandType::SetPx(
                cmd_vec[1].clone(),
                cmd_vec[2].clone(),
                ttl,
            ))
        }
        "get" => {
            if cmd_vec.len() != 2 {
                return Err(anyhow::anyhow!("Invalid RESP command"));
            }
            Ok(RespCommandType::Get(cmd_vec[1].clone()))
        }
        _ => Err(anyhow::anyhow!("Unknown command")),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn command_parser_test() {
        assert_eq!(
            command_parser("*1\r\n$4\r\nping\r\n").unwrap(),
            RespCommandType::Ping
        );
        assert_eq!(
            command_parser("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n").unwrap(),
            RespCommandType::Set("key".to_string(), "value".to_string())
        );
        assert_eq!(
            command_parser(
                "*5\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$3\r\n100\r\n"
            )
            .unwrap(),
            RespCommandType::SetPx("key".to_string(), "value".to_string(), 100)
        );
        assert_eq!(
            command_parser("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n").unwrap(),
            RespCommandType::Get("key".to_string())
        );
    }
}
