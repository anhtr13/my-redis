use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::Mutex;

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
        let mut lock = self.kv_bucket.lock().await;
        lock.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
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

    pub async fn del(&self, key: &str) -> Option<String> {
        let mut lock = self.kv_bucket.lock().await;
        lock.remove(key).map(|obj| obj.value)
    }

    pub async fn lpush(&self, key: String, mut vals: Vec<String>) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        for val in vals {
            list.push_front(val);
        }
        list.len()
    }

    pub async fn rpush(&self, key: String, mut vals: Vec<String>) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.entry(key).or_insert(VecDeque::new());
        list.extend(vals);
        list.len()
    }

    pub async fn lrange(&self, key: &str, mut start: i64, mut stop: i64) -> Vec<String> {
        let mut lock = self.list_bucket.lock().await;
        let Some(list) = lock.get(key) else {
            return Vec::new();
        };
        if start < 0 {
            start += list.len() as i64;
        }
        if stop < 0 {
            stop += list.len() as i64;
        }
        let mut start = start.max(0) as usize;
        let stop = (stop.max(0) as usize + 1).min(list.len());
        let mut res = Vec::new();
        for val in list.iter().skip(start) {
            if start >= stop {
                break;
            }
            start += 1;
            res.push(val.to_owned());
        }
        res
    }

    pub async fn llen(&self, key: &str) -> usize {
        let mut lock = self.list_bucket.lock().await;
        let list = lock.get(key);
        match list {
            Some(list) => list.len(),
            None => 0,
        }
    }

    pub async fn lpop(&self, key: &str, mut n: usize) -> Option<Vec<String>> {
        let mut lock = self.list_bucket.lock().await;
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

    pub async fn key_type(&self, key: &str) -> String {
        let lock = self.kv_bucket.lock().await;
        if lock.contains_key(key) {
            return "string".to_string();
        }
        let lock = self.list_bucket.lock().await;
        if lock.contains_key(key) {
            return "list".to_string();
        }
        "none".to_string()
    }
}
