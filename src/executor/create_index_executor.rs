use super::Executor;
use crate::common::SendableDataBlockStream;
use crate::errors::{MySQLError, MySQLResult};
use crate::planner::CreateIndexPlan;
use crate::session::SessionRef;
use crate::store::Storage;
use crate::table::schema::{ColumnInfo, IndexType};
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
        let mut index_info = self.plan.index_info.clone();
        meta.max_index_id += 1;
        index_info.id = meta.max_index_id;
        meta.indices.push(Arc::new(index_info));
        let mut columns = vec![];
        for col in meta.columns.iter() {
            let mut column: ColumnInfo = col.as_ref().clone();
            column.key = IndexType::Index;
            columns.push(Arc::new(column));
        }
        meta.columns = columns;
        let table_name = meta.name.clone();
        session.replace_table(table_name, meta);
        Ok(vec![])
    }
}
