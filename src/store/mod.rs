use crate::errors::MySQLResult;
use async_trait::async_trait;

mod mem;

#[async_trait]
pub trait Storage: Sized + Sync + Send {
    type Txn: Transaction;
    async fn insert(&self, key: &[u8], value: &[u8]) -> MySQLResult<()>;
    async fn get(&self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>>;
    fn new_transaction(&self) -> MySQLResult<Self::Txn>;
}

#[async_trait]
pub trait Transaction: Sized + Send {
    async fn commit(&mut self) -> MySQLResult<()>;
    async fn put(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()>;
    async fn delete(&mut self, key: &[u8]) -> MySQLResult<()>;
    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>>;
    async fn scan(&mut self, start: &[u8], end: &[u8]) -> MySQLResult<Vec<Vec<u8>>>;
    fn get_start_time(&self) -> Option<u64>;
}
