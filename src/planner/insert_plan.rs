use crate::common::EncodeValue;
use crate::session::SessionRef;
use crate::table::schema::DataSchema;
use crate::table::table::TableSource;
use std::sync::Arc;

pub struct InsertPlan {
    pub table: Arc<TableSource>,
    pub values: Vec<Vec<EncodeValue>>,
    pub schema: DataSchema,
    pub session: SessionRef,
}
