use super::create_index_executor::CreateIndexExecutor;
use super::create_table_executor::CreateTableExecutor;
use super::insert_executor::InsertExecutor;
use super::point_get_executor::PointGetExecutor;
use crate::executor::Executor;
use crate::planner::PlanNode;
use crate::session::SessionRef;
use crate::store::Storage;
use std::sync::Arc;

pub struct ExecutorBuilder {}

impl ExecutorBuilder {
    pub fn build(
        plan: PlanNode,
        session: SessionRef,
        storage: Arc<dyn Storage>,
    ) -> Box<dyn Executor> {
        match plan {
            PlanNode::CreateTable(p) => Box::new(CreateTableExecutor::new(p, session)),
            PlanNode::PointGet(p) => Box::new(PointGetExecutor::new(p, storage)),
            PlanNode::Insert(p) => Box::new(InsertExecutor::new(p, storage)),
            PlanNode::CreateIndex(p) => Box::new(CreateIndexExecutor::new(p, session, storage)),
            _ => unimplemented!(),
        }
    }
}
