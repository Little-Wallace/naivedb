use crate::errors::MySQLResult;
use crate::session::SessionRef;
use crate::store::{Storage, Transaction, TransactionOptions};
use crate::transaction::TransactionContext;
use async_trait::async_trait;
use std::sync::Arc;

pub struct AutoCommitContext {
    storage: Arc<dyn Storage>,
}

impl AutoCommitContext {
    pub fn new(storage: Arc<dyn Storage>) -> AutoCommitContext {
        AutoCommitContext { storage }
    }
}

#[async_trait::async_trait]
impl TransactionContext for AutoCommitContext {
    async fn check_constants(&mut self, key: &[u8]) -> MySQLResult<bool> {
        Ok(true)
    }

    async fn write(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        let opts = TransactionOptions {
            pessimistic: false,
            no_timestamp: false,
        };
        let mut txn = self.storage.new_transaction(&opts)?;
        txn.put(key, value).await?;
        txn.commit().await
    }

    async fn commit(&mut self) -> MySQLResult<()> {
        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        self.storage.get(key).await
    }
}
