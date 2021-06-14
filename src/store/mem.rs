use crate::errors::MySQLResult;
use crate::store::{Storage, Transaction, TransactionOptions};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct MemStorage {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<Operation>>>>,
    last_commit_ts: AtomicU64,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            last_commit_ts: AtomicU64::new(1),
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Delete(u64),
    Put(Vec<u8>, u64),
}

pub struct MemTransaction {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<Operation>>>>,
    cache: BTreeMap<Vec<u8>, Operation>,
    start_ts: u64,
}

impl MemTransaction {
    pub fn new(
        data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<Operation>>>>,
        start_ts: u64,
    ) -> MemTransaction {
        MemTransaction {
            data,
            cache: BTreeMap::default(),
            start_ts,
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    async fn get(&self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        let ops = data.get(key);
        if let Some(values) = ops {
            match values.first() {
                Some(Operation::Put(v, _)) => return Ok(Some(v.clone())),
                _ => (),
            }
        }
        Ok(None)
    }

    async fn new_transaction(&self, _: &TransactionOptions) -> MySQLResult<Box<dyn Transaction>> {
        let start_ts = self.last_commit_ts.load(Ordering::Acquire);
        Ok(Box::new(MemTransaction::new(self.data.clone(), start_ts)))
    }
}

#[async_trait]
impl Transaction for MemTransaction {
    async fn commit(&mut self) -> MySQLResult<()> {
        let mut data = self.data.lock().unwrap();
        for (key, value) in self.cache.iter() {
            if let Some(v) = data.get_mut(key) {
                v.push(value.clone());
            } else {
                data.insert(key.clone(), vec![value.clone()]);
            }
        }
        Ok(())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        self.cache
            .insert(key.to_vec(), Operation::Put(value.to_vec(), self.start_ts));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> MySQLResult<()> {
        self.cache
            .insert(key.to_vec(), Operation::Delete(self.start_ts));
        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        if let Some(value) = self.cache.get(key) {
            if let Operation::Put(v, _) = value {
                return Ok(Some(v.clone()));
            } else {
                return Ok(None);
            }
        }
        let data = self.data.lock().unwrap();
        if let Some(values) = data.get(key) {
            if values.is_empty() {
                return Ok(None);
            }
            let idx = values.len();
            while idx > 0 {
                match &values[idx - 1] {
                    Operation::Put(v, ts) => {
                        if *ts <= self.start_ts {
                            return Ok(Some(v.clone()));
                        }
                    }
                    Operation::Delete(ts) => {
                        if *ts <= self.start_ts {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    async fn scan(&mut self, _start: &[u8], _end: &[u8]) -> MySQLResult<Vec<Vec<u8>>> {
        Ok(vec![])
    }

    fn get_start_time(&self) -> u64 {
        self.start_ts
    }
}
