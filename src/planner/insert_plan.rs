use crate::common::EncodeValue;
use crate::table::schema::DataSchema;
use std::sync::Arc;
use crate::table::table::TableSource;
use crate::session::SessionRef;

pub struct InsertPlan {
    pub table: Arc<TableSource>,
    pub values: Vec<Vec<EncodeValue>>,
    pub schema: DataSchema,
    pub session: SessionRef,
}
