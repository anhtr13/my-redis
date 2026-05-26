use std::collections::HashMap;

use tokio::sync::Mutex;

#[allow(unused)]
pub struct StreamEntry {
    id: String,
    values: Vec<String>,
}

pub struct StreamBucket(pub Mutex<HashMap<String, Vec<StreamEntry>>>);

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
