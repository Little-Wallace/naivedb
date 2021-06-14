use crate::common::{DataBlock, SendableDataBlockStream};
use crate::errors::MySQLResult;
use crate::executor::Executor;
use crate::planner::PointGetPlan;
use crate::store::{Storage, TransactionOptions};
use crate::transaction::{AutoCommitContext, OptimisticTransactionContext};
use std::sync::Arc;

pub struct PointGetExecutor {
    plan: PointGetPlan,
    storage: Arc<dyn Storage>,
}

#[async_trait::async_trait]
impl Executor for PointGetExecutor {
    fn name(&self) -> &str {
        "PointGetExecutor"
    }

    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream> {
        let transaction = {
            let mut session = self.plan.session.lock().unwrap();
            session.take_transaction()
        };
        let txn = if let Some(txn) = transaction {
            txn
        } else {
            if self.plan.index_info.primary {
                let table = self.plan.table.clone();
                let mut ctx = AutoCommitContext::new(self.storage.clone());
                let ret = table
                    .read_record_by_index(
                        &mut ctx,
                        self.plan.index_info.as_ref(),
                        self.plan.select_columns.as_ref(),
                        &self.plan.index_value,
                    )
                    .await?;
                let schema = self.plan.select_columns.clone();
                return Ok(vec![DataBlock {
                    schema,
                    data: vec![ret],
                }]);
            } else {
                let opts = TransactionOptions {
                    pessimistic: false,
                    no_timestamp: false,
                };
                self.storage.new_transaction(&opts)?
            }
        };
        let mut ctx = OptimisticTransactionContext::new(txn);
        let ret = self.execute_transaction(&mut ctx).await;
        self.plan
            .session
            .lock()
            .unwrap()
            .set_transaction(ctx.take_transaction());
        ret
    }
}

impl PointGetExecutor {
    pub fn new(plan: PointGetPlan, storage: Arc<dyn Storage>) -> PointGetExecutor {
        PointGetExecutor { plan, storage }
    }

    async fn execute_transaction(
        &mut self,
        ctx: &mut OptimisticTransactionContext,
    ) -> MySQLResult<SendableDataBlockStream> {
        let table = self.plan.table.clone();
        if let Some(handle) = table
            .read_handle_from_index(ctx, self.plan.index_info.as_ref(), &self.plan.index_value)
            .await?
        {
            let ret = table
                .read_record(ctx, self.plan.select_columns.as_ref(), &handle)
                .await?;
            let schema = self.plan.select_columns.clone();
            Ok(vec![DataBlock {
                schema,
                data: vec![ret],
            }])
        } else {
            Ok(vec![])
        }
    }
}
