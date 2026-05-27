use anyhow::Result;

use crate::encoding::Encoding;

pub enum Command {
    Ping,
    Echo(String),
    Set(String, String, u64),
    Get(String),
    Lpush(String, Vec<String>),
    Rpush(String, Vec<String>),
    Lrange(String, i64, i64),
    Llen(String),
    Lpop(String, usize),
    Blpop(String, f64),
    Type(String),
    XAdd(String, String, Vec<String>),
    XRange(String, String, String),
    Other,
}

impl Command {
    pub fn from(data: Encoding) -> Result<Self> {
        let Encoding::Array(array) = data else {
            anyhow::bail!("unknow command")
        };
        let mut args: Vec<_> = array
            .into_iter()
            .filter_map(|arg| match arg {
                Encoding::BulkString(value) => Some(value),
                _ => None,
            })
            .collect();
        anyhow::ensure!(args.len() > 0);
        match args[0].to_uppercase().as_str() {
            "PING" => Ok(Self::Ping),
            "ECHO" => {
                anyhow::ensure!(args.len() == 2);
                Ok(Self::Echo(args.pop().unwrap()))
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
                Ok(Self::Set(key, val, ttl))
            }
            "GET" => {
                anyhow::ensure!(args.len() == 2);
                Ok(Self::Get(args.pop().unwrap()))
            }
            "LPUSH" => {
                anyhow::ensure!(args.len() >= 3);
                let vals = args.split_off(2);
                let key = args.pop().unwrap();
                Ok(Self::Lpush(key, vals))
            }
            "RPUSH" => {
                anyhow::ensure!(args.len() >= 3);
                let vals = args.split_off(2);
                let key = args.pop().unwrap();
                Ok(Self::Rpush(key, vals))
            }
            "LRANGE" => {
                anyhow::ensure!(args.len() == 4);
                let stop: i64 = args.pop().unwrap().parse()?;
                let start: i64 = args.pop().unwrap().parse()?;
                let key = args.pop().unwrap();
                Ok(Self::Lrange(key, start, stop))
            }
            "LLEN" => {
                anyhow::ensure!(args.len() == 2);
                Ok(Self::Llen(args.pop().unwrap()))
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
                Ok(Self::Lpop(key, n))
            }
            "BLPOP" => {
                anyhow::ensure!(args.len() == 3);
                let timeout: f64 = args.pop().unwrap().parse()?;
                let key = args.pop().unwrap();
                Ok(Self::Blpop(key, timeout))
            }
            "TYPE" => {
                anyhow::ensure!(args.len() == 2);
                let key = args.pop().unwrap();
                Ok(Self::Type(key))
            }
            "XADD" => {
                anyhow::ensure!(args.len() >= 3);
                let entries = args.split_off(3);
                let id = args.pop().unwrap();
                let key = args.pop().unwrap();
                Ok(Self::XAdd(key, id, entries))
            }
            "XRANGE" => {
                anyhow::ensure!(args.len() == 4);
                let stop = args.pop().unwrap();
                let start = args.pop().unwrap();
                let key = args.pop().unwrap();
                Ok(Self::XRange(key, start, stop))
            }
            _ => Ok(Self::Other),
        }
    }
}
