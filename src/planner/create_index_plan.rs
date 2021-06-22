use crate::table::schema::IndexInfo;
use crate::table::TableSource;
use std::sync::Arc;

pub struct CreateIndexPlan {
    pub index_info: IndexInfo,
    pub table: Arc<TableSource>,
}
