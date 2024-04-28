use super::{RespCommandType, RespParsingState, RespType};
use anyhow;

// we assume that the input is always a RESP array
pub fn command_parser(input: &mut [u8]) -> anyhow::Result<Vec<RespCommandType>> {
    let mut iter = input.iter().enumerate().peekable();
    let mut parsing_state = RespParsingState::ParsingMeta;

    let mut current_resp_type = RespType::Array;

    let mut cmd_vec = Vec::new();
    let mut cmd_number = 0;

    let mut resp_vec = Vec::new();
    while let Some((pos, c)) = iter.next() {
        let c = *c as char;
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
                        .take_while(|(_pos, c)| (**c as char) != '\r')
                        .map(|(_pos, c)| *c as char)
                        .collect();
                    // expect next is \r\n
                    cmd_vec = Vec::new();
                    cmd_vec.push(simple_string);
                    resp_vec.push(process_command_vec(cmd_vec));
                    cmd_vec = Vec::new();
                    parsing_state = RespParsingState::ParsingMeta;
                }
            }
            '0'..='9' => {
                if parsing_state == RespParsingState::ParsingMeta {
                    let start = pos;
                    let mut end = pos;
                    while iter
                        .next_if(|&(_pos, ch)| (*ch as char).is_numeric())
                        .is_some()
                    {
                        end += 1;
                    }
                    if end < input.len() {
                        end += 1;
                    }
                    // turn out to str first
                    let num = std::str::from_utf8(&input[start..end])
                        .to_owned()?
                        .parse::<usize>()?;
                    // println!("collect number: {}", num);

                    if iter.next_if_eq(&(end, &('\r' as u8))).is_some()
                        && iter.next_if_eq(&(end + 1, &('\n' as u8))).is_some()
                    {
                        if current_resp_type == RespType::Array {
                            cmd_number = num;
                        } else {
                            parsing_state = RespParsingState::ParsingData;
                            let str_data = iter
                                .by_ref()
                                .take(num)
                                .map(|(_pos, ch)| *ch as char)
                                .collect::<String>();

                            cmd_vec.push(str_data);
                            // new command
                            // include string case
                            if cmd_number <= 1 {
                                resp_vec.push(process_command_vec(cmd_vec));
                                cmd_vec = Vec::new();
                                parsing_state = RespParsingState::ParsingMeta;
                            } else {
                                cmd_number -= 1;
                            }
                        }
                    } else {
                        return Err(anyhow::anyhow!("Invalid RESP command"));
                    }
                }
            }
            _ => {
                if c == '\r' {
                    if iter.next_if_eq(&(pos + 1, &('\n' as u8))).is_some() {
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

    // println!("cmd_vec: {:?}", cmd_vec);

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

    use super::*;

    #[test]
    fn command_parser_test() {
        unsafe {
            let mut input1 = String::from("*1\r\n$4\r\nping\r\n");
            let mut input2 = String::from("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
            let mut input3 = String::from(
                "*5\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$3\r\n100\r\n",
            );
            let mut input4 = String::from("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n");
            let mut input5 =
                String::from("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n");
            let mut input6 = String::from("$3\r\nabc\r\n");

            let mut d1 = unsafe { input1.as_bytes_mut() };
            let mut d2 = unsafe { input2.as_bytes_mut() };
            let mut d3 = unsafe { input3.as_bytes_mut() };
            let mut d4 = unsafe { input4.as_bytes_mut() };
            let mut d5 = unsafe { input5.as_bytes_mut() };
            let mut d6 = unsafe { input6.as_bytes_mut() };

            assert_eq!(command_parser(&mut d1).unwrap()[0], RespCommandType::Ping);

            assert_eq!(
                command_parser(&mut d2).unwrap()[0],
                RespCommandType::Set("key".to_string(), "value".to_string())
            );

            assert_eq!(
                command_parser(&mut d3).unwrap()[0],
                RespCommandType::SetPx("key".to_string(), "value".to_string(), 100)
            );

            assert_eq!(
                command_parser(&mut d4).unwrap()[0],
                RespCommandType::Get("key".to_string())
            );

            assert_eq!(command_parser(&mut d5).unwrap()[0], RespCommandType::Error);

            assert_eq!(command_parser(&mut d6).unwrap()[0], RespCommandType::Error);
        }
    }

    #[test]
    fn command_parser_test2() {
        let mut input1 = String::from("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n");
        let mut input2 = String::from("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        let mut d1 = unsafe { input1.as_bytes_mut() };
        let mut d2 = unsafe { input2.as_bytes_mut() };

        let mut vec1 = Vec::new();
        vec1.push(RespCommandType::Set("foo".to_string(), "123".to_string()));
        vec1.push(RespCommandType::Set("bar".to_string(), "456".to_string()));
        assert_eq!(command_parser(&mut d1).unwrap(), vec1,);

        let mut vec2 = Vec::new();
        vec2.push(RespCommandType::Replconf(("getack").to_string()));
        assert_eq!(command_parser(&mut d2).unwrap(), vec2,);
    }
}
