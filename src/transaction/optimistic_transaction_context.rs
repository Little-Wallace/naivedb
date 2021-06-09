
use crate::errors::MySQLResult;
use crate::session::SessionRef;
use crate::store::{Storage, Transaction, TransactionOptions};
use crate::transaction::TransactionContext;
use async_trait::async_trait;
use std::sync::Arc;

pub struct OptimisticTransactionContext {
    txn: Box<dyn Transaction>,
}

impl OptimisticTransactionContext {
    pub fn new(txn: Box<dyn Transaction>) -> OptimisticTransactionContext {
        OptimisticTransactionContext {
            txn
        }
    }
}

#[async_trait::async_trait]
impl TransactionContext for OptimisticTransactionContext {
    async fn check_constants(&mut self, _: &[u8]) -> MySQLResult<bool> {
        Ok(true)
    }

    async fn write(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        self.txn.put(key, value).await
    }

    async fn commit(&mut self) -> MySQLResult<()> {
        self.txn.commit().await
    }

    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        self.txn.get(key).await
    }
}


