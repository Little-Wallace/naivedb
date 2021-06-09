use crate::table::schema::TableInfo;
use std::sync::Arc;

pub struct CreateTablePlan {
    pub table_info: Arc<TableInfo>,
}
