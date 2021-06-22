use super::Executor;
use crate::common::SendableDataBlockStream;
use crate::errors::MySQLResult;
use crate::planner::CreateTablePlan;
use crate::session::SessionRef;
use crate::table::table::TableSource;
use std::sync::Arc;

pub struct CreateTableExecutor {
    plan: CreateTablePlan,
    session: SessionRef,
}

#[async_trait::async_trait]
impl Executor for CreateTableExecutor {
    fn name(&self) -> &str {
        "CreateTableExecutor"
    }

    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream> {
        let mut session = self.session.lock().unwrap();
        // TODO: run the realy DDL change.
        session.add_table(
            self.plan.table_info.name.clone(),
            self.plan.table_info.clone(),
        );
        Ok(vec![])
    }
}

impl CreateTableExecutor {
    pub fn new(plan: CreateTablePlan, session: SessionRef) -> Self {
        Self { plan, session }
    }
}
