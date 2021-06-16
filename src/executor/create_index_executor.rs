use super::Executor;
use crate::common::SendableDataBlockStream;
use crate::errors::{MySQLError, MySQLResult};
use crate::planner::CreateIndexPlan;
use crate::session::SessionRef;
use crate::store::{Storage, TransactionOptions};
use crate::table::TableSource;
use std::sync::Arc;

pub struct CreateIndexExecutor {
    plan: CreateIndexPlan,
    session: SessionRef,
    storage: Arc<dyn Storage>,
}

impl CreateIndexExecutor {
    pub fn new(
        plan: CreateIndexPlan,
        session: SessionRef,
        storage: Arc<dyn Storage>,
    ) -> CreateIndexExecutor {
        CreateIndexExecutor {
            plan,
            session,
            storage,
        }
    }
}

#[async_trait::async_trait]
impl Executor for CreateIndexExecutor {
    fn name(&self) -> &str {
        "CreateIndexExecutor"
    }

    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream> {
        let mut session = self.session.lock().unwrap();
        // TODO: run the realy DDL change.
        let mut meta = self.plan.table.clone_meta();
        for index in meta.indices.iter() {
            if index.columns == self.plan.index_info.columns {
                return Err(MySQLError::IndexExist);
            }
        }
        let mut index_info = self.plan.index_info.as_ref().clone();
        meta.max_index_id += 1;
        index_info.id = meta.max_index_id;
        meta.indices.push(Arc::new(index_info));
        let table_name = meta.name.clone();
        let table = TableSource::new(Arc::new(meta));
        session.add_table(table_name, Arc::new(table));
        Ok(vec![])
    }
}
