mod autocommit_context;
mod optimistic_transaction_context;

use crate::errors::MySQLResult;
use async_trait::async_trait;
pub use autocommit_context::AutoCommitContext;
pub use optimistic_transaction_context::OptimisticTransactionContext;

#[async_trait]
pub trait TransactionContext: Send {
    async fn check_constants(&mut self, key: &[u8]) -> MySQLResult<bool>;
    async fn write(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()>;
    async fn commit(&mut self) -> MySQLResult<()>;
    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>>;
}
