use crate::errors::MySQLResult;
use async_trait::async_trait;

mod mem;
mod tikv;

pub use mem::MemStorage;
pub use tikv::{TiKVConfig, TiKVStorage};

pub struct TransactionOptions {
    pub pessimistic: bool,
}

#[async_trait]
pub trait Storage: Sync + Send {
    async fn get(&self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>>;
    async fn new_transaction(&self, opts: &TransactionOptions)
        -> MySQLResult<Box<dyn Transaction>>;
}

#[async_trait]
pub trait Transaction: Send {
    async fn commit(&mut self) -> MySQLResult<()>;
    async fn put(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()>;
    async fn delete(&mut self, key: &[u8]) -> MySQLResult<()>;
    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>>;
    async fn scan(&mut self, start: &[u8], end: &[u8]) -> MySQLResult<Vec<Vec<u8>>>;
    fn get_start_time(&self) -> u64;
}
