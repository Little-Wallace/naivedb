use crate::errors::MySQLResult;
use crate::store::Transaction;
use crate::transaction::TransactionContext;

pub struct OptimisticTransactionContext {
    txn: Box<dyn Transaction>,
}

impl OptimisticTransactionContext {
    pub fn new(txn: Box<dyn Transaction>) -> OptimisticTransactionContext {
        OptimisticTransactionContext { txn }
    }

    pub fn take_transaction(self) -> Box<dyn Transaction> {
        self.txn
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
