pub mod key_val;
pub mod list;
pub mod stream;

use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::storage::{key_val::KVBucket, list::ListBucket, stream::StreamBucket};

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
