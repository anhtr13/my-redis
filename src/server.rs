use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use tokio::sync::RwLock;

use crate::data_type::DataType;

pub struct Server {
    keyval: Arc<RwLock<HashMap<DataType, DataType>>>,
}

#[allow(unused)]
impl Server {
    pub fn new() -> Self {
        Self {
            keyval: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set(&self, args: &mut Vec<DataType>) -> Result<()> {
        anyhow::ensure!(!args.is_empty() && args.len().is_multiple_of(2));
        let mut lock = self.keyval.write().await;
        while let Some(val) = args.pop()
            && let Some(key) = args.pop()
        {
            lock.insert(key, val);
        }
        Ok(())
    }

    pub async fn get(&self, key: &DataType) -> Option<DataType> {
        self.keyval.read().await.get(key).cloned()
    }
}
