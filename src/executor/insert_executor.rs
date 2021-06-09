use std::sync::Arc;

use crate::common::SendableDataBlockStream;
use crate::errors::MySQLResult;
use crate::executor::Executor;
use crate::planner::InsertPlan;
use crate::session::SessionRef;
use crate::store::Storage;

pub struct InsertExecutor {
    storage: Arc<dyn Storage>,
    session: SessionRef,
    plan: InsertPlan,
}

#[async_trait::async_trait]
impl Executor for InsertExecutor {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream> {
        Ok(vec![])
    }
}
