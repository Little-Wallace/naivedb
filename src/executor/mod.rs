mod create_table_executor;
mod executor_builder;
mod insert_executor;
mod point_get_executor;

use crate::common::SendableDataBlockStream;
use crate::errors::MySQLResult;
use crate::table::schema::DataSchemaRef;
pub use point_get_executor::PointGetExecutor;
pub use create_table_executor::CreateTableExecutor;
pub use insert_executor::InsertExecutor;
pub use executor_builder::ExecutorBuilder;

#[async_trait::async_trait]
pub trait Executor: Send + 'static {
    fn name(&self) -> &str;
    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream>;
}
