use std::{
    collections::HashMap,
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub struct StreamId {
    millis_time: u128,
    sequence_num: u64,
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.millis_time, self.sequence_num)
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.millis_time == other.millis_time {
            return Some(self.sequence_num.cmp(&other.sequence_num));
        }
        Some(self.millis_time.cmp(&other.millis_time))
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub values: Vec<String>,
}

pub struct StreamBucket(pub Mutex<HashMap<String, Vec<StreamEntry>>>);

impl StreamBucket {
    pub async fn add(&self, key: String, id: String, values: Vec<String>) -> Result<String> {
        let (millis_time, sequence_num) = self.parse_id(&key, &id).await?;
        self.0
            .lock()
            .await
            .entry(key)
            .or_insert(Vec::new())
            .push(StreamEntry {
                id: StreamId {
                    millis_time,
                    sequence_num,
                },
                values,
            });
        Ok(format!("{millis_time}-{sequence_num}"))
    }

    pub async fn range(&self, key: &str, start: &str, stop: &str) -> Result<Vec<StreamEntry>> {
        let start_id = match start {
            "-" => StreamId {
                millis_time: 0,
                sequence_num: 0,
            },
            start => {
                let (time, number) = start.split_once('-').unwrap_or((start, "0"));
                let time: u128 = time.parse()?;
                let number: u64 = number.parse()?;
                StreamId {
                    millis_time: time,
                    sequence_num: number,
                }
            }
        };

        let stop_id = match stop {
            "+" => StreamId {
                millis_time: u128::MAX,
                sequence_num: u64::MAX,
            },
            stop => {
                let (time, number) = stop.split_once('-').unwrap_or((stop, "999"));
                let time: u128 = time.parse()?;
                let number: u64 = number.parse()?;
                StreamId {
                    millis_time: time,
                    sequence_num: number,
                }
            }
        };

        match self.0.lock().await.get(key) {
            Some(stream) => {
                let res: Vec<_> = stream
                    .iter()
                    .filter_map(|entry| {
                        if entry.id >= start_id && entry.id <= stop_id {
                            Some(entry.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(res)
            }
            None => Ok(Vec::new()),
        }
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
            anyhow::ensure!(
                millis_time >= last_entry.id.millis_time,
                "is equal or smaller than the target stream top item"
            );
            if millis_time == last_entry.id.millis_time {
                if sequence_num == 0 {
                    sequence_num = last_entry.id.sequence_num + 1;
                } else {
                    anyhow::ensure!(
                        sequence_num > last_entry.id.sequence_num,
                        "is equal or smaller than the target stream top item"
                    )
                }
            }
        }
        Ok((millis_time, sequence_num))
    }
}
