use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync,
};

use crate::{
    storage::Storage,
    {command::Command, encoding::Encoding},
};

pub async fn handle_connection(
    storage: Arc<Storage>,
    stream: TcpStream,
    _sockaddr: SocketAddr,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let data = Encoding::deserialize(&mut reader).await?;
        match Command::from(data) {
            Ok(cmd) => match cmd {
                Command::Ping => writer.write_all(b"+PONG\r\n").await?,
                Command::Echo(echo_string) => {
                    let res = format!("${}\r\n{}\r\n", echo_string.len(), echo_string).into_bytes();
                    writer.write_all(&res).await?;
                }
                Command::Set(key, val, ttl) => {
                    storage.kv_bucket.set(key, val, ttl).await;
                    writer.write_all(b"+OK\r\n").await?;
                }
                Command::Get(key) => {
                    let value = storage.kv_bucket.get(&key).await;
                    match value {
                        Some(value) => {
                            let res = format!("${}\r\n{}\r\n", value.len(), value).into_bytes();
                            writer.write_all(&res).await?;
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                Command::Lpush(key, vals) => {
                    let n = storage.list_bucket.lpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::Rpush(key, vals) => {
                    let n = storage.list_bucket.rpush(key, vals).await;
                    writer.write_all(format!(":{n}\r\n").as_bytes()).await?;
                }
                Command::Lrange(key, start, stop) => {
                    let vals: Vec<_> = storage
                        .list_bucket
                        .range(&key, start, stop)
                        .await
                        .into_iter()
                        .map(Encoding::BulkString)
                        .collect();
                    let res = Encoding::Array(vals);
                    writer.write_all(&res.serialize()).await?;
                }
                Command::Llen(key) => {
                    let length = storage.list_bucket.len(&key).await;
                    writer
                        .write_all(format!(":{length}\r\n").as_bytes())
                        .await?;
                }
                Command::Lpop(key, n) => {
                    let res = storage.list_bucket.pop(&key, n).await;
                    match res {
                        Some(mut arr) => {
                            if arr.is_empty() {
                                writer.write_all(b"$-1\r\n").await?;
                            } else if arr.len() == 1 {
                                let res = Encoding::BulkString(arr.pop().unwrap());
                                writer.write_all(&res.serialize()).await?;
                            } else {
                                let value: Vec<_> =
                                    arr.into_iter().map(Encoding::BulkString).collect();
                                let res = Encoding::Array(value);
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
                                val = storage.list_bucket.pop(&key, 1) => {
                                    if let Some(val) = val && let Some(val) = val.into_iter().next() {
                                        let res = Encoding::Array(vec![Encoding::BulkString(key), Encoding::BulkString(val)]);
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
                            let val = storage.list_bucket.pop(&key, 1).await;
                            if let Some(val) = val
                                && let Some(val) = val.into_iter().next()
                            {
                                let res = Encoding::Array(vec![
                                    Encoding::BulkString(key),
                                    Encoding::BulkString(val),
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
                Command::XAdd(key, id, values) => {
                    match storage.stream_bucket.add(key, id, values).await {
                        Ok(id) => {
                            writer
                                .write_all(&Encoding::BulkString(id).serialize())
                                .await?;
                        }
                        Err(e) => {
                            let err_msg =
                                Encoding::SimpleError(format!("ERR The ID specified in XADD {e}"));
                            writer.write_all(&err_msg.serialize()).await?;
                        }
                    };
                }
                Command::Other => writer.write_all(b"+OK\r\n").await?,
            },
            Err(e) => {
                let err = Encoding::BulkError(e.to_string());
                writer.write_all(&err.serialize()).await?;
            }
        }
    }
}
