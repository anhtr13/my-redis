use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::Mutex;

#[derive(Clone)]
pub struct KVObject {
    value: String,
    expired_at: u128,
}

pub struct KVBucket(pub Mutex<HashMap<String, KVObject>>);

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
