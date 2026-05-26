use std::collections::{HashMap, VecDeque};

use tokio::sync::Mutex;

pub struct ListBucket(pub Mutex<HashMap<String, VecDeque<String>>>);

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
