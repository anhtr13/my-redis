use anyhow::Result;

use crate::protocol::data_type::DataType;

pub enum Command {
    Ping,
    Echo { echo_string: String },
    Set { key: String, val: String, ttl: u64 },
    Get { key: String },
    LPush { key: String, vals: Vec<String> },
    RPush { key: String, vals: Vec<String> },
    LRange { key: String, start: i64, stop: i64 },
    LLen { key: String },
    LPop { key: String, n: usize },
    Other,
}

impl Command {
    pub fn from(data: DataType) -> Result<Self> {
        if let DataType::Array(value) = data {
            let mut args: Vec<_> = value
                .into_iter()
                .filter_map(|arg| match arg {
                    DataType::BulkString(value) => Some(value),
                    _ => None,
                })
                .collect();

            anyhow::ensure!(args.len() > 0);

            return match args[0].to_uppercase().as_str() {
                "PING" => Ok(Self::Ping),
                "ECHO" => {
                    anyhow::ensure!(args.len() == 2);
                    Ok(Self::Echo {
                        echo_string: args.pop().unwrap(),
                    })
                }
                "SET" => {
                    anyhow::ensure!(args.len() >= 3);
                    let mut ttl = 0u64;
                    let mut prev_arg = String::from("");
                    while args.len() > 3 {
                        let arg = args.pop().unwrap();
                        match arg.to_uppercase().as_str() {
                            "PX" => {
                                ttl = prev_arg.parse()?;
                                prev_arg = String::from("");
                            }
                            "EX" => {
                                ttl = prev_arg.parse()?;
                                ttl *= 1000;
                                prev_arg = String::from("");
                            }
                            _ => prev_arg = arg,
                        }
                    }
                    let val = args.pop().unwrap();
                    let key = args.pop().unwrap();
                    Ok(Self::Set { key, val, ttl })
                }
                "GET" => {
                    anyhow::ensure!(args.len() == 2);
                    Ok(Self::Get {
                        key: args.pop().unwrap(),
                    })
                }
                "LPUSH" => {
                    anyhow::ensure!(args.len() >= 3);
                    let vals = args.split_off(2);
                    let key = args.pop().unwrap();
                    Ok(Self::LPush { key, vals })
                }
                "RPUSH" => {
                    anyhow::ensure!(args.len() >= 3);
                    let vals = args.split_off(2);
                    let key = args.pop().unwrap();
                    Ok(Self::RPush { key, vals })
                }
                "LRANGE" => {
                    anyhow::ensure!(args.len() == 4);
                    let stop: i64 = args.pop().unwrap().parse()?;
                    let start: i64 = args.pop().unwrap().parse()?;
                    let key = args.pop().unwrap();
                    Ok(Self::LRange { key, start, stop })
                }
                "LLEN" => {
                    anyhow::ensure!(args.len() == 2);
                    Ok(Self::LLen {
                        key: args.pop().unwrap(),
                    })
                }
                "LPOP" => {
                    anyhow::ensure!(args.len() == 2 || args.len() == 3);
                    let n: usize = if args.len() == 2 {
                        1
                    } else {
                        args.pop().unwrap().parse()?
                    };
                    anyhow::ensure!(n >= 1);
                    let key = args.pop().unwrap();
                    Ok(Self::LPop { key, n })
                }
                _ => Ok(Self::Other),
            };
        }
        anyhow::bail!("unknow command")
    }
}
