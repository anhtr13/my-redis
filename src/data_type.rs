use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    io::Write,
    pin::Pin,
};

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[derive(Debug)]
pub enum DataType {
    SimpleString {
        value: String,
    },
    SimpleError {
        value: String,
    },
    Integer {
        value: i64,
    },
    BulkString {
        value: String,
    },
    BulkError {
        value: String,
    },
    Array {
        value: Vec<DataType>,
    },
    Null,
    Boolean {
        value: bool,
    },
    Double {
        value: f64,
    },

    /// store BigNumber as a Vector of u32 values with each value >= 0 and <= 999_999_999
    /// ex: x = BigNumber{value:[a,b,c], positive=false} => x = -(a + b * 10^9 + c * 10^(9*2))
    BigNumber {
        value: Vec<u32>,
        positive: bool,
    },
    VerbatimString {
        encoding: String,
        data: String,
    },
    Map {
        value: HashMap<DataType, DataType>,
    },
    Attribute {
        value: HashMap<DataType, DataType>,
    },
    Set {
        value: HashSet<DataType>,
    },
    Push {
        value: Vec<DataType>,
    },
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString { value } => {
                write!(f, "{value}")
            }
            Self::SimpleError { value } => {
                write!(f, "-{value}")
            }
            Self::Integer { value } => {
                write!(f, "{value}")
            }
            Self::BulkString { value } => {
                write!(f, "{value}")
            }
            Self::BulkError { value } => {
                write!(f, "!{value}")
            }
            Self::Array { value } => {
                let mut s = String::new();
                for val in value {
                    s.push_str(&val.to_string());
                    s.push(' ');
                }
                s.pop();
                write!(f, "[{s}]")
            }
            Self::Null => {
                write!(f, "NULL")
            }
            Self::Boolean { value } => {
                write!(f, "{value}")
            }
            Self::Double { value } => {
                write!(f, "{value}")
            }
            Self::BigNumber { value, positive } => {
                let marker = if *positive { "" } else { "-" };
                let mut s = String::new();
                for n in value {
                    s = format!("{n}{s}");
                }
                write!(f, "{marker}{s}")
            }
            Self::VerbatimString { encoding, data } => {
                write!(f, "{encoding}:{data}")
            }
            Self::Map { value } => {
                let mut s = String::from("%");
                for (k, v) in value {
                    s.push_str(&k.to_string());
                    s.push(':');
                    s.push_str(&v.to_string());
                    s.push(' ');
                }
                s.pop();
                write!(f, "{{{s}}}")
            }
            Self::Attribute { value } => {
                let mut s = String::from("%");
                for (k, v) in value {
                    s.push_str(&k.to_string());
                    s.push(':');
                    s.push_str(&v.to_string());
                    s.push(' ');
                }
                s.pop();
                write!(f, "{{{s}}}")
            }
            Self::Set { value } => {
                let mut s = String::from("%");
                for k in value {
                    s.push_str(&k.to_string());
                    s.push(' ');
                }
                s.pop();
                write!(f, "[{s}]")
            }
            Self::Push { value } => {
                let mut s = String::new();
                for val in value {
                    s.push_str(&val.to_string());
                    s.push(' ');
                }
                s.pop();
                write!(f, "[{s}]")
            }
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::Double { value: d1 }, DataType::Double { value: d2 }) => {
                if d1.is_nan() && d2.is_nan() {
                    return true;
                }
                d1 == d2
            }
            _ => self == other,
        }
    }
}

impl Eq for DataType {}

impl Hash for DataType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::SimpleString { value } => {
                '+'.hash(state);
                value.hash(state);
            }
            Self::SimpleError { value } => {
                '-'.hash(state);
                value.hash(state);
            }
            Self::Integer { value } => {
                value.hash(state);
            }
            Self::BulkString { value } => {
                '$'.hash(state);
                value.hash(state);
            }
            Self::BulkError { value } => {
                '!'.hash(state);
                value.hash(state);
            }
            Self::Array { value } => {
                value.hash(state);
            }
            Self::Null => {
                '_'.hash(state);
            }
            Self::Boolean { value } => {
                value.hash(state);
            }
            Self::Double { value } => {
                value.to_string().hash(state);
            }
            Self::BigNumber { value, positive } => {
                positive.hash(state);
                value.hash(state);
            }
            Self::VerbatimString { encoding, data } => {
                encoding.hash(state);
                data.hash(state);
            }
            Self::Map { value } => {
                let mut s = String::from("%");
                for (k, v) in value {
                    s.push_str(&k.to_string());
                    s.push(':');
                    s.push_str(&v.to_string());
                    s.push(' ');
                }
                s.pop();
                format!("${{{s}}}").hash(state);
            }
            Self::Attribute { value } => {
                let mut s = String::from("%");
                for (k, v) in value {
                    s.push_str(&k.to_string());
                    s.push(':');
                    s.push_str(&v.to_string());
                    s.push(' ');
                }
                s.pop();
                format!("|{{{s}}}").hash(state);
            }
            Self::Set { value } => {
                let mut s = String::from("%");
                for k in value {
                    s.push_str(&k.to_string());
                    s.push(' ');
                }
                s.pop();
                format!("~[{s}]").hash(state);
            }
            Self::Push { value } => {
                '>'.hash(state);
                value.hash(state);
            }
        }
    }
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl DataType {
    pub fn deserialize<'a, R: AsyncBufReadExt + Unpin + Send>(
        reader: &'a mut R,
    ) -> BoxFuture<'a, anyhow::Result<Self>> {
        Box::pin(async move {
            let mut buffer = String::new();
            reader.read_line(&mut buffer).await?;

            anyhow::ensure!(buffer.len() > 3);

            let lead = buffer.chars().nth(0).unwrap();

            match lead {
                '+' => Ok(Self::SimpleString {
                    value: buffer[1..buffer.len() - 2].to_owned(),
                }),
                '-' => Ok(Self::SimpleError {
                    value: buffer[1..buffer.len() - 2].to_owned(),
                }),
                ':' => Ok(Self::Integer {
                    value: buffer[1..buffer.len() - 2].to_owned().parse()?,
                }),
                '$' => {
                    let size = &buffer[1..buffer.len() - 2];
                    if size == "-1" {
                        return Ok(Self::Null);
                    }

                    let size: usize = size.parse()?;
                    anyhow::ensure!(size > 0);

                    let mut data = vec![0u8; size + 2];
                    reader.read_exact(&mut data).await?;

                    let clrf = data.split_off(data.len() - 2);
                    anyhow::ensure!(&clrf == b"\r\n");

                    let value = String::from_utf8(data)?;
                    Ok(Self::BulkString { value })
                }
                '*' => {
                    let size = &buffer[1..buffer.len() - 2];
                    if size == "-1" {
                        return Ok(Self::Null);
                    }
                    let size: usize = size.parse()?;
                    let mut value = Vec::new();
                    for _ in 0..size {
                        value.push(Self::deserialize(reader).await?);
                    }

                    Ok(Self::Array { value })
                }
                '_' => Ok(Self::Null),
                '#' => match buffer.chars().nth(1) {
                    Some('t') => Ok(Self::Boolean { value: true }),
                    Some('f') => Ok(Self::Boolean { value: false }),
                    _ => anyhow::bail!("wrong format: not a boolean value"),
                },
                ',' => {
                    let value = &buffer[1..buffer.len() - 2];
                    match value {
                        "inf" => Ok(Self::Double {
                            value: f64::INFINITY,
                        }),
                        "-inf" => Ok(Self::Double {
                            value: f64::NEG_INFINITY,
                        }),
                        "nan" => Ok(Self::Double { value: f64::NAN }),
                        _ => Ok(Self::Double {
                            value: value.parse()?,
                        }),
                    }
                }
                '(' => {
                    let mut num = &buffer[1..buffer.len() - 2];
                    let positive = match num.chars().nth(0) {
                        Some('-') => false,
                        _ => true,
                    };
                    if !positive {
                        num = &num[1..];
                    }
                    let mut value = Vec::new();
                    while num.len() > 0 {
                        let i = num.len() - num.len().min(9);
                        let n: u32 = num[i..].parse()?;
                        value.push(n);
                        num = &num[..i];
                    }
                    Ok(Self::BigNumber { value, positive })
                }
                '!' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    anyhow::ensure!(size > 0);

                    let mut data = vec![0u8; size + 2];
                    reader.read_exact(&mut data).await?;

                    let clrf = data.split_off(data.len() - 2);
                    anyhow::ensure!(&clrf == b"\r\n");

                    let value = String::from_utf8(data)?;
                    Ok(Self::BulkError { value })
                }
                '=' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    anyhow::ensure!(size > 0);

                    let mut data = vec![0u8; size + 2];
                    reader.read_exact(&mut data).await?;

                    let clrf = data.split_off(data.len() - 2);
                    anyhow::ensure!(&clrf == b"\r\n");

                    let value = String::from_utf8(data)?;
                    let Some((encoding, data)) = value.split_once(':') else {
                        anyhow::bail!("invaid VerbatimString format");
                    };
                    Ok(Self::VerbatimString {
                        encoding: encoding.to_owned(),
                        data: data.to_owned(),
                    })
                }
                '%' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashMap::new();
                    for _ in 0..size {
                        let k = Self::deserialize(reader).await?;
                        let v = Self::deserialize(reader).await?;
                        value.insert(k, v);
                    }

                    Ok(Self::Map { value })
                }
                '|' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashMap::new();
                    for _ in 0..size {
                        let k = Self::deserialize(reader).await?;
                        let v = Self::deserialize(reader).await?;
                        value.insert(k, v);
                    }

                    Ok(Self::Attribute { value })
                }
                '~' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashSet::new();
                    for _ in 0..size {
                        value.insert(Self::deserialize(reader).await?);
                    }
                    Ok(Self::Set { value })
                }
                '>' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = Vec::new();
                    for _ in 0..size {
                        value.push(Self::deserialize(reader).await?);
                    }
                    Ok(Self::Push { value })
                }
                _ => anyhow::bail!("unknow lead byte"),
            }
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        match self {
            Self::SimpleString { value } => {
                write!(res, "+{value}\r\n").expect("failed writing to buffer");
            }
            Self::SimpleError { value } => {
                write!(res, "-{value}\r\n").expect("failed writing to buffer");
            }
            Self::Integer { value } => {
                write!(res, ":{value}\r\n").expect("failed writing to buffer");
            }
            Self::BulkString { value } => {
                write!(res, "${}\r\n{}\r\n", value.len(), value).expect("failed writing to buffer");
            }
            Self::BulkError { value } => {
                write!(res, "!{}\r\n{}\r\n", value.len(), value).expect("failed writing to buffer");
            }
            Self::Array { value } => {
                write!(res, "*{}\r\n", value.len()).expect("failed writing to buffer");
                for v in value {
                    res.extend(v.serialize());
                }
            }
            Self::Null => {
                write!(res, "_\r\n").expect("failed writing to buffer");
            }
            Self::Boolean { value } => {
                let c = if *value { 't' } else { 'f' };
                write!(res, "#{c}\r\n").expect("failed writing to buffer");
            }
            Self::Double { value } => {
                let value = if value.is_nan() {
                    "nan"
                } else if *value == f64::INFINITY {
                    "inf"
                } else if *value == f64::NEG_INFINITY {
                    "-inf"
                } else {
                    &value.to_string()
                };
                write!(res, ",{value}\r\n").expect("failed writing to buffer");
            }
            Self::BigNumber { value, positive } => {
                let marker = if *positive { "" } else { "-" };
                let mut num = String::new();
                for n in value {
                    num = format!("{n}{num}");
                }
                write!(res, "({marker}{num}\r\n").expect("failed writing to buffer");
            }
            Self::VerbatimString { encoding, data } => {
                write!(
                    res,
                    "={}\r\n{encoding}:{data}\r\n",
                    encoding.len() + data.len() + 1
                )
                .expect("failed writing to buffer");
            }
            Self::Map { value } => {
                write!(res, "%{}\r\n", value.len()).expect("failed writing to buffer");
                for (k, v) in value {
                    res.extend(k.serialize());
                    res.extend(v.serialize());
                }
            }
            Self::Attribute { value } => {
                write!(res, "|{}\r\n", value.len()).expect("failed writing to buffer");
                for (k, v) in value {
                    res.extend(k.serialize());
                    res.extend(v.serialize());
                }
            }
            Self::Set { value } => {
                write!(res, "~{}\r\n", value.len()).expect("failed writing to buffer");
                for v in value {
                    res.extend(v.serialize());
                }
            }
            Self::Push { value } => {
                write!(res, ">{}\r\n", value.len()).expect("failed writing to buffer");
                for v in value {
                    res.extend(v.serialize());
                }
            }
        }
        res
    }
}
