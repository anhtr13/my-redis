use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Mutex,
};

use crate::protocol::{Command, DataType};

#[derive(Clone)]
struct KVObject {
    value: String,
    expired_at: u128,
}

pub struct Storage {
    kv_bucket: Arc<Mutex<HashMap<String, KVObject>>>,
    list_bucket: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
}

#[allow(unused)]
impl Storage {
    pub fn new() -> Self {
        Self {
            kv_bucket: Arc::new(Mutex::new(HashMap::new())),
            list_bucket: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn kv_set(&self, key: String, value: String, ttl: u64) {
        let expired_at = if ttl > 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + ttl as u128
        } else {
            0
        };
        let value = KVObject { value, expired_at };
        let mut lock = self.kv_bucket.lock().await;
        lock.insert(key, value);
    }

    pub async fn kv_get(&self, key: &str) -> Option<String> {
        let mut lock = self.kv_bucket.lock().await;
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

    pub async fn kv_del(&self, key: &str) -> Option<String> {
        let mut lock = self.kv_bucket.lock().await;
        lock.remove(key).map(|obj| obj.value)
    }

    pub async fn list_lpush(&self, key: String, mut vals: Vec<String>) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        for val in vals {
            list.push_front(val);
        }
        list.len()
    }

    pub async fn list_rpush(&self, key: String, mut vals: Vec<String>) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        list.extend(vals);
        list.len()
    }

    pub async fn list_lrange(&self, key: String, mut start: i64, mut stop: i64) -> Vec<String> {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        if start < 0 {
            start += list.len() as i64;
        }
        if stop < 0 {
            stop += list.len() as i64;
        }
        let start = start.max(0) as usize;
        let stop = (stop.max(0) as usize + 1).min(list.len());
        if list.is_empty() || start >= stop {
            return Vec::new();
        }
        list.make_contiguous()[start..stop].to_vec()
    }

    pub async fn list_llen(&self, key: String) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.get(&key);
        match list {
            Some(list) => list.len(),
            None => 0,
        }
    }

    pub async fn list_lpop(&self, key: String, mut n: usize) -> Option<Vec<String>> {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.get_mut(&key);
        if let Some(list) = list {
            let mut res = Vec::new();
            while let Some(val) = list.pop_front()
                && n > 0
            {
                res.push(val);
                n -= 1;
            }
            return Some(res);
        }
        None
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
                        .map(|value| DataType::BulkString { value })
                        .collect();
                    let res = DataType::Array { value: vals };
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
                                let res = DataType::BulkString {
                                    value: arr.pop().unwrap(),
                                };
                                writer.write_all(&res.serialize()).await?;
                            } else {
                                let value: Vec<_> = arr
                                    .into_iter()
                                    .map(|val| DataType::BulkString { value: val })
                                    .collect();
                                let res = DataType::Array { value };
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
