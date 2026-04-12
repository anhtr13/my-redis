use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync,
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
                Command::Echo(echo_string) => {
                    let res = format!("${}\r\n{}\r\n", echo_string.len(), echo_string).into_bytes();
                    writer.write_all(&res).await?;
                }
                Command::Set(key, val, ttl) => {
                    storage.set(key, val, ttl).await;
                    writer.write_all(b"+OK\r\n").await?;
                }
                Command::Get(key) => {
                    let value = storage.get(&key).await;
                    match value {
                        Some(value) => {
                            let res = format!("${}\r\n{}\r\n", value.len(), value).into_bytes();
                            writer.write_all(&res).await?;
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                Command::Lpush(key, vals) => {
                    let n = storage.lpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::Rpush(key, vals) => {
                    let n = storage.rpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::Lrange(key, start, stop) => {
                    let vals: Vec<_> = storage
                        .lrange(&key, start, stop)
                        .await
                        .into_iter()
                        .map(DataType::BulkString)
                        .collect();
                    let res = DataType::Array(vals);
                    writer.write_all(&res.serialize()).await?;
                }
                Command::Llen(key) => {
                    let length = storage.llen(&key).await;
                    writer
                        .write_all(format!(":{length}\r\n").as_bytes())
                        .await?;
                }
                Command::Lpop(key, n) => {
                    let res = storage.lpop(&key, n).await;
                    match res {
                        Some(mut arr) => {
                            if arr.is_empty() {
                                writer.write_all(b"$-1\r\n").await?;
                            } else if arr.len() == 1 {
                                let res = DataType::BulkString(arr.pop().unwrap());
                                writer.write_all(&res.serialize()).await?;
                            } else {
                                let value: Vec<_> =
                                    arr.into_iter().map(DataType::BulkString).collect();
                                let res = DataType::Array(value);
                                writer.write_all(&res.serialize()).await?;
                            }
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                Command::Blpop(key, timeout) => {
                    if timeout > 0.0 {
                        let (tx_notify, mut rx_notify) = sync::oneshot::channel::<bool>();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs_f64(timeout)).await;
                            tx_notify.send(true).expect("cannot send stop signal");
                        });
                        loop {
                            tokio::select! {
                                val = storage.lpop(&key, 1) => {
                                    if let Some(val) = val && let Some(val) = val.into_iter().next() {
                                        let res = DataType::Array(vec![DataType::BulkString(key), DataType::BulkString(val)]);
                                        writer.write_all(&res.serialize()).await?;
                                        break
                                    }
                                }
                                _ = &mut rx_notify => {
                                    writer.write_all(b"*-1\r\n").await?;
                                    break;
                                }
                            }
                        }
                    } else {
                        loop {
                            let val = storage.lpop(&key, 1).await;
                            if let Some(val) = val
                                && let Some(val) = val.into_iter().next()
                            {
                                let res = DataType::Array(vec![
                                    DataType::BulkString(key),
                                    DataType::BulkString(val),
                                ]);
                                writer.write_all(&res.serialize()).await?;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                Command::Type(key) => {
                    let val = storage.key_type(&key).await;
                    let res = format!("+{val}\r\n").into_bytes();
                    writer.write_all(&res).await?;
                }
                Command::Other => writer.write_all(b"+OK\r\n").await?,
            },
            Err(e) => {
                let err = DataType::BulkError(e.to_string());
                writer.write_all(&err.serialize()).await?;
            }
        }
    }
}
