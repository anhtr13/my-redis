use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Mutex,
};

use crate::protocol::{ClientCommand, DataType};

#[derive(Clone)]
pub struct SetObject {
    value: String,
    expired_at: u128,
}

pub struct Storage {
    set: Arc<Mutex<HashMap<String, SetObject>>>,
}

#[allow(unused)]
impl Storage {
    pub fn new() -> Self {
        Self {
            set: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn set(&self, key: String, value: String, ttl: u64) {
        let expired_at = if ttl > 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + ttl as u128
        } else {
            0
        };
        let value = SetObject { value, expired_at };
        let mut lock = self.set.lock().await;
        lock.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut lock = self.set.lock().await;
        let obj = lock.get(key);
        let mut still_alive = true;
        if let Some(obj) = obj {
            if obj.expired_at == 0 {
                return Some(obj.value.clone());
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            still_alive = obj.expired_at >= now;
            if still_alive {
                return Some(obj.value.clone());
            }
        }
        if !still_alive {
            lock.remove(key);
        }
        None
    }

    pub async fn delete(&self, key: &str) -> Option<String> {
        let mut lock = self.set.lock().await;
        lock.remove(key).map(|obj| obj.value)
    }
}

pub async fn handle_connection(
    storage: Arc<Storage>,
    stream: TcpStream,
    _sockaddr: SocketAddr,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let data = DataType::deserialize(&mut reader).await?;
        match ClientCommand::from(data) {
            Ok(cmd) => match cmd {
                ClientCommand::Ping => writer.write_all(b"+PONG\r\n").await?,
                ClientCommand::Echo { echo_string } => {
                    let res = format!("${}\r\n{}\r\n", echo_string.len(), echo_string).into_bytes();
                    writer.write_all(&res).await?;
                }
                ClientCommand::Set { key, val, ttl } => {
                    storage.set(key, val, ttl).await;
                    writer.write_all(b"+OK\r\n").await?;
                }
                ClientCommand::Get { key } => {
                    let value = storage.get(&key).await;
                    match value {
                        Some(value) => {
                            let res = format!("${}\r\n{}\r\n", value.len(), value).into_bytes();
                            writer.write_all(&res).await?;
                        }
                        None => writer.write_all(b"$-1\r\n").await?,
                    }
                }
                ClientCommand::Other => writer.write_all(b"+OK\r\n").await?,
            },
            Err(e) => {
                let err = e.to_string();
                let res = format!("!{}\r\n{}", err.len(), err).into_bytes();
                writer.write_all(&res).await?;
            }
        }
    }
}
