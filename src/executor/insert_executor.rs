use std::sync::Arc;

use crate::common::SendableDataBlockStream;
use crate::errors::MySQLResult;
use crate::executor::Executor;
use crate::planner::InsertPlan;
use crate::store::{Storage, TransactionOptions};
use crate::table::EncoderRow;
use crate::transaction::{OptimisticTransactionContext, TransactionContext};

pub struct InsertExecutor {
    storage: Arc<dyn Storage>,
    plan: InsertPlan,
}

#[async_trait::async_trait]
impl Executor for InsertExecutor {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream> {
        let opts = TransactionOptions {
            pessimistic: false,
            no_timestamp: false,
        };
        let txn = self.storage.new_transaction(&opts)?;
        let mut ctx = OptimisticTransactionContext::new(txn);
        let mut row = EncoderRow::default();
        for r in self.plan.values.drain(..) {
            self.plan
                .table
                .add_record(&mut ctx, &mut row, &self.plan.schema.columns, r)
                .await?;
            row.clear();
        }
        ctx.commit().await?;
        Ok(vec![])
    }
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan, storage: Arc<dyn Storage>) -> InsertExecutor {
        InsertExecutor { storage, plan }
    }
}
