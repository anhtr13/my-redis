use std::{
    collections::{HashMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::Mutex;

#[derive(Clone)]
struct KVObject {
    value: String,
    expired_at: u128,
}

#[allow(unused)]
struct StreamEntry {
    id: String,
    values: Vec<String>,
}

pub struct KVBucket(Mutex<HashMap<String, KVObject>>);
pub struct ListBucket(Mutex<HashMap<String, VecDeque<String>>>);
pub struct StreamBucket(Mutex<HashMap<String, Vec<StreamEntry>>>);

impl KVBucket {
    pub async fn set(&self, key: String, value: String, ttl: u64) {
        let expired_at = match ttl {
            0 => 0,
            _ => {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    + ttl as u128
            }
        };
        let value = KVObject { value, expired_at };
        let mut lock = self.0.lock().await;
        lock.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut lock = self.0.lock().await;
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

    #[allow(unused)]
    pub async fn del(&self, key: &str) -> Option<String> {
        let mut lock = self.0.lock().await;
        lock.remove(key).map(|obj| obj.value)
    }
}

impl ListBucket {
    pub async fn lpush(&self, key: String, vals: Vec<String>) -> usize {
        let mut lock = self.0.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        for val in vals {
            list.push_front(val);
        }
        list.len()
    }

    pub async fn rpush(&self, key: String, vals: Vec<String>) -> usize {
        let mut lock = self.0.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        list.extend(vals);
        list.len()
    }

    pub async fn range(&self, key: &str, mut start: i64, mut stop: i64) -> Vec<String> {
        let lock = self.0.lock().await;
        let Some(list) = lock.get(key) else {
            return Vec::new();
        };
        if start < 0 {
            start += list.len() as i64;
        }
        if stop < 0 {
            stop += list.len() as i64;
        }
        let skip = start.max(0) as usize;
        let length = (stop.max(0) as usize + 1).min(list.len()) - skip;
        let res: Vec<_> = list
            .iter()
            .skip(skip)
            .take(length)
            .map(|val| val.to_owned())
            .collect();
        res
    }

    pub async fn len(&self, key: &str) -> usize {
        let lock = self.0.lock().await;
        let list = lock.get(key);
        match list {
            Some(list) => list.len(),
            None => 0,
        }
    }

    pub async fn pop(&self, key: &str, mut n: usize) -> Option<Vec<String>> {
        let mut lock = self.0.lock().await;
        let list = lock.get_mut(key);
        if let Some(list) = list {
            let mut res = Vec::new();
            while n > 0
                && let Some(val) = list.pop_front()
            {
                res.push(val);
                n -= 1;
            }
            return Some(res);
        }
        None
    }
}

impl StreamBucket {
    pub async fn add(&self, key: String, id: String, values: Vec<String>) -> String {
        self.0
            .lock()
            .await
            .entry(key)
            .or_insert(Vec::new())
            .push(StreamEntry {
                id: id.clone(),
                values,
            });
        id
    }
}

pub struct Storage {
    pub kv_bucket: KVBucket,
    pub list_bucket: ListBucket,
    pub stream_bucket: StreamBucket,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            kv_bucket: KVBucket(Mutex::new(HashMap::new())),
            list_bucket: ListBucket(Mutex::new(HashMap::new())),
            stream_bucket: StreamBucket(Mutex::new(HashMap::new())),
        }
    }

    pub async fn key_type(&self, key: &str) -> String {
        let lock = self.kv_bucket.0.lock().await;
        if lock.contains_key(key) {
            return "string".to_string();
        }
        let lock = self.list_bucket.0.lock().await;
        if lock.contains_key(key) {
            return "list".to_string();
        }
        let lock = self.stream_bucket.0.lock().await;
        if lock.contains_key(key) {
            return "stream".to_string();
        }
        "none".to_string()
    }
}
