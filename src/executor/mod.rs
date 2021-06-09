mod create_table_executor;
mod executor_builder;
mod insert_executor;
mod point_get_executor;

use crate::common::SendableDataBlockStream;
use crate::errors::MySQLResult;
use crate::table::schema::DataSchemaRef;

#[async_trait::async_trait]
pub trait Executor: Send + 'static {
    fn name(&self) -> &str;
    async fn execute(&mut self) -> MySQLResult<SendableDataBlockStream>;
}
