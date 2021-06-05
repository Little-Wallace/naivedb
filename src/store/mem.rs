use crate::errors::MySQLResult;
use crate::store::{Storage, Transaction};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
pub struct MemStorage {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

pub struct MemTransaction {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    cache: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemTransaction {
    pub fn new(data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>) -> MemTransaction {
        MemTransaction {
            data,
            cache: BTreeMap::default(),
        }
    }
}

#[async_trait]
impl Storage for MemStorage {
    type Txn = MemTransaction;

    async fn insert(&self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        Ok(self.data.lock().unwrap().get(key).map(|v| v.clone()))
    }

    fn new_transaction(&self) -> MySQLResult<Self::Txn> {
        Ok(MemTransaction::new(self.data.clone()))
    }
}

#[async_trait]
impl Transaction for MemTransaction {
    async fn commit(&mut self) -> MySQLResult<()> {
        Ok(())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> MySQLResult<()> {
        self.data.lock().unwrap().remove(key);
        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        Ok(self.data.lock().unwrap().get(key).map(|v| v.clone()))
    }

    async fn scan(&mut self, start: &[u8], end: &[u8]) -> MySQLResult<Vec<Vec<u8>>> {
        Ok(vec![])
    }

    fn get_start_time(&self) -> Option<u64> {
        None
    }
}
