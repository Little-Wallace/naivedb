use crate::table::schema::IndexInfo;
use crate::table::TableSource;
use std::sync::Arc;

pub struct CreateIndexPlan {
    pub index_info: Arc<IndexInfo>,
    pub table: Arc<TableSource>,
}
