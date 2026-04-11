use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{
    protocol::{command::Command, data_type::DataType},
    server::storage::Storage,
};

pub async fn handle_connection(
    storage: Arc<Storage>,
    stream: TcpStream,
    _sockaddr: SocketAddr,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let data = DataType::deserialize(&mut reader).await?;
        match Command::from(data) {
            Ok(cmd) => match cmd {
                Command::Ping => writer.write_all(b"+PONG\r\n").await?,
                Command::Echo { echo_string } => {
                    let res = format!("${}\r\n{}\r\n", echo_string.len(), echo_string).into_bytes();
                    writer.write_all(&res).await?;
                }
                Command::Set { key, val, ttl } => {
                    storage.kv_set(key, val, ttl).await;
                    writer.write_all(b"+OK\r\n").await?;
                }
                Command::Get { key } => {
                    let value = storage.kv_get(&key).await;
                    match value {
                        Some(value) => {
                            let res = format!("${}\r\n{}\r\n", value.len(), value).into_bytes();
                            writer.write_all(&res).await?;
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                Command::LPush { key, vals } => {
                    let n = storage.list_lpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::RPush { key, vals } => {
                    let n = storage.list_rpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::LRange { key, start, stop } => {
                    let vals: Vec<_> = storage
                        .list_lrange(key, start, stop)
                        .await
                        .into_iter()
                        .map(|val| DataType::BulkString(val))
                        .collect();
                    let res = DataType::Array(vals);
                    writer.write_all(&res.serialize()).await?;
                }
                Command::LLen { key } => {
                    let length = storage.list_llen(key).await;
                    writer
                        .write_all(format!(":{length}\r\n").as_bytes())
                        .await?;
                }
                Command::LPop { key, n } => {
                    let res = storage.list_lpop(key, n).await;
                    match res {
                        Some(mut arr) => {
                            if arr.is_empty() {
                                writer.write_all(b"$-1\r\n").await?;
                            } else if arr.len() == 1 {
                                let res = DataType::BulkString(arr.pop().unwrap());
                                writer.write_all(&res.serialize()).await?;
                            } else {
                                let value: Vec<_> = arr
                                    .into_iter()
                                    .map(|val| DataType::BulkString(val))
                                    .collect();
                                let res = DataType::Array(value);
                                writer.write_all(&res.serialize()).await?;
                            }
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                Command::Other => writer.write_all(b"+OK\r\n").await?,
            },
            Err(e) => {
                let err = e.to_string();
                let res = format!("!{}\r\n{}", err.len(), err).into_bytes();
                writer.write_all(&res).await?;
            }
        }
    }
}
