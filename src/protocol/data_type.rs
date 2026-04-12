use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    pin::Pin,
};

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone)]
pub enum DataType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    BulkError(String),
    Array(Vec<DataType>),
    NullBulkArray,
    Null,
    Boolean(bool),
    Double(f64),

    /// store BigNumber as a Vector of u32 values with each value >= 0 and <= 999_999_999
    /// ex: x = BigNumber{value:[a,b,c], positive=false} => x = -(a + b * 10^9 + c * 10^(9*2))
    BigNumber(bool, Vec<u32>),
    VerbatimString(String, String), // (encoding, data)
    Map(HashMap<DataType, DataType>),
    Attribute(HashMap<DataType, DataType>),
    Set(HashSet<DataType>),
    Push(Vec<DataType>),
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.serialize() == other.serialize()
    }
}

impl Eq for DataType {}

impl Hash for DataType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.serialize().hash(state);
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
                '+' => Ok(Self::SimpleString(buffer[1..buffer.len() - 2].to_owned())),
                '-' => Ok(Self::SimpleError(buffer[1..buffer.len() - 2].to_owned())),
                ':' => Ok(Self::Integer(
                    buffer[1..buffer.len() - 2].to_owned().parse()?,
                )),
                '$' => {
                    let size = &buffer[1..buffer.len() - 2];
                    if size == "-1" {
                        return Ok(Self::NullBulkString);
                    }

                    let size: usize = size.parse()?;
                    anyhow::ensure!(size > 0);

                    let mut data = vec![0u8; size + 2];
                    reader.read_exact(&mut data).await?;

                    let clrf = data.split_off(data.len() - 2);
                    anyhow::ensure!(&clrf == b"\r\n");

                    let value = String::from_utf8(data)?;
                    Ok(Self::BulkString(value))
                }
                '*' => {
                    let size = &buffer[1..buffer.len() - 2];
                    if size == "-1" {
                        return Ok(Self::NullBulkArray);
                    }
                    let size: usize = size.parse()?;
                    let mut value = Vec::new();
                    for _ in 0..size {
                        value.push(Self::deserialize(reader).await?);
                    }
                    Ok(Self::Array(value))
                }
                '_' => Ok(Self::Null),
                '#' => match buffer.chars().nth(1) {
                    Some('t') => Ok(Self::Boolean(true)),
                    Some('f') => Ok(Self::Boolean(false)),
                    _ => anyhow::bail!("wrong format: not a boolean value"),
                },
                ',' => {
                    let value = &buffer[1..buffer.len() - 2];
                    match value {
                        "inf" => Ok(Self::Double(f64::INFINITY)),
                        "-inf" => Ok(Self::Double(f64::NEG_INFINITY)),
                        "nan" => Ok(Self::Double(f64::NAN)),
                        _ => Ok(Self::Double(value.parse()?)),
                    }
                }
                '(' => {
                    let mut num = &buffer[1..buffer.len() - 2];
                    let positive = !matches!(num.chars().nth(0), Some('-'));
                    if !positive {
                        num = &num[1..];
                    }
                    let mut value = Vec::new();
                    while !num.is_empty() {
                        let i = num.len() - num.len().min(9);
                        let n: u32 = num[i..].parse()?;
                        value.push(n);
                        num = &num[..i];
                    }
                    Ok(Self::BigNumber(positive, value))
                }
                '!' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    anyhow::ensure!(size > 0);

                    let mut data = vec![0u8; size + 2];
                    reader.read_exact(&mut data).await?;

                    let clrf = data.split_off(data.len() - 2);
                    anyhow::ensure!(&clrf == b"\r\n");

                    let value = String::from_utf8(data)?;
                    Ok(Self::BulkError(value))
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
                    Ok(Self::VerbatimString(encoding.to_owned(), data.to_owned()))
                }
                '%' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashMap::new();
                    for _ in 0..size {
                        let k = Self::deserialize(reader).await?;
                        let v = Self::deserialize(reader).await?;
                        value.insert(k, v);
                    }
                    Ok(Self::Map(value))
                }
                '|' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashMap::new();
                    for _ in 0..size {
                        let k = Self::deserialize(reader).await?;
                        let v = Self::deserialize(reader).await?;
                        value.insert(k, v);
                    }
                    Ok(Self::Attribute(value))
                }
                '~' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = HashSet::new();
                    for _ in 0..size {
                        value.insert(Self::deserialize(reader).await?);
                    }
                    Ok(Self::Set(value))
                }
                '>' => {
                    let size: usize = buffer[1..buffer.len() - 2].parse()?;
                    let mut value = Vec::new();
                    for _ in 0..size {
                        value.push(Self::deserialize(reader).await?);
                    }
                    Ok(Self::Push(value))
                }
                _ => anyhow::bail!("unknow data format"),
            }
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::SimpleString(value) => format!("+{value}\r\n").into_bytes(),
            Self::SimpleError(value) => format!("-{value}\r\n").into_bytes(),
            Self::Integer(value) => format!(":{value}\r\n").into_bytes(),
            Self::BulkString(value) => format!("${}\r\n{}\r\n", value.len(), value).into_bytes(),
            Self::NullBulkString => b"$-1\r\n".to_vec(),
            Self::BulkError(value) => format!("!{}\r\n{}\r\n", value.len(), value).into_bytes(),
            Self::Array(value) => {
                let mut res = format!("*{}\r\n", value.len()).into_bytes();
                let inner_arr: Vec<_> = value.iter().flat_map(|v| v.serialize()).collect();
                res.extend(inner_arr);
                res
            }
            Self::NullBulkArray => b"*-1\r\n".to_vec(),
            Self::Null => b"_\r\n".to_vec(),

            Self::Boolean(value) => {
                if *value {
                    b"#t\r\n".to_vec()
                } else {
                    b"#f\r\n".to_vec()
                }
            }
            Self::Double(value) => {
                let value = if value.is_nan() {
                    "nan"
                } else if *value == f64::INFINITY {
                    "inf"
                } else if *value == f64::NEG_INFINITY {
                    "-inf"
                } else {
                    &value.to_string()
                };
                format!(",{value}\r\n").into_bytes()
            }
            Self::BigNumber(positive, value) => {
                let marker = if *positive { "" } else { "-" };
                let mut num = String::new();
                value.iter().for_each(|chunk| {
                    num = format!("{:09}{}", chunk, num);
                });
                let num = num.trim_start_matches("0");
                format!("({marker}{num}\r\n").into_bytes()
            }
            Self::VerbatimString(encoding, data) => format!(
                "={}\r\n{encoding}:{data}\r\n",
                encoding.len() + data.len() + 1
            )
            .into_bytes(),
            Self::Map(value) => {
                let mut res = format!("%{}\r\n", value.len()).into_bytes();
                let inner: Vec<_> = value
                    .iter()
                    .flat_map(|(k, v)| [k.serialize(), v.serialize()].concat())
                    .collect();
                res.extend(inner);
                res
            }
            Self::Attribute(value) => {
                let mut res = format!("|{}\r\n", value.len()).into_bytes();
                let inner: Vec<_> = value
                    .iter()
                    .flat_map(|(k, v)| [k.serialize(), v.serialize()].concat())
                    .collect();
                res.extend(inner);
                res
            }
            Self::Set(value) => {
                let mut res = format!("~{}\r\n", value.len()).into_bytes();
                let inner: Vec<_> = value.iter().flat_map(|v| v.serialize()).collect();
                res.extend(inner);
                res
            }
            Self::Push(value) => {
                let mut res = format!(">{}\r\n", value.len()).into_bytes();
                let inner: Vec<_> = value.iter().flat_map(|v| v.serialize()).collect();
                res.extend(inner);
                res
            }
        }
    }
}
