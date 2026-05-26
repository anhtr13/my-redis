use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::Mutex;

#[allow(unused)]
pub struct StreamEntry {
    id: String,
    values: Vec<String>,
}

pub struct StreamBucket(pub Mutex<HashMap<String, Vec<StreamEntry>>>);

impl StreamBucket {
    pub async fn add(&self, key: String, id: String, values: Vec<String>) -> Result<String> {
        let (millis_time, sequence_num) = self.parse_id(&key, &id).await?;
        let id = format!("{millis_time}-{sequence_num}");
        self.0
            .lock()
            .await
            .entry(key)
            .or_insert(Vec::new())
            .push(StreamEntry {
                id: id.clone(),
                values,
            });
        Ok(id)
    }

    async fn parse_id(&self, key: &str, id: &str) -> Result<(u128, u64)> {
        if id == "*" {
            let millis_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            return Ok((millis_time, 0));
        }
        let Some((millis_time, sequence_num)) = id.split_once('-') else {
            anyhow::bail!("has invalid format");
        };
        let millis_time = match millis_time {
            "*" => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            _ => millis_time.parse::<u128>()?,
        };
        let mut sequence_num = match sequence_num {
            "*" if millis_time > 0 => 0,
            "*" if millis_time == 0 => 1,
            _ => sequence_num.parse::<u64>()?,
        };
        if millis_time == 0 && sequence_num == 0 {
            anyhow::bail!("must be greater than 0-0")
        }
        if let Some(stream) = self.0.lock().await.get(key)
            && let Some(last_entry) = stream.last()
        {
            let (last_millis_time, last_sequence_num) = last_entry.id.split_once('-').unwrap();
            let last_millis_time: u128 = last_millis_time.parse().unwrap();
            let last_sequence_num: u64 = last_sequence_num.parse().unwrap();
            anyhow::ensure!(
                millis_time >= last_millis_time,
                "is equal or smaller than the target stream top item"
            );
            if millis_time == last_millis_time {
                if sequence_num == 0 {
                    sequence_num = last_sequence_num + 1;
                } else {
                    anyhow::ensure!(
                        sequence_num > last_sequence_num,
                        "is equal or smaller than the target stream top item"
                    )
                }
            }
        }
        Ok((millis_time, sequence_num))
    }
}
