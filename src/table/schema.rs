use sqlparser::ast::DataType;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableState {
    Tombstone,
    Public,
    WriteOnly,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnState {
    Tombstone,
    Public,
    WriteOnly,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub id: u64,
    pub name: String,
    pub columns: Vec<Arc<ColumnInfo>>,
    pub indices: Vec<Arc<IndexInfo>>,
    pub state: TableState,
    pub pk_is_handle: bool,
    pub auto_inc_id: u64,
    pub update_ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub id: u64,
    pub name: String,
    pub offset: usize,
    pub data_type: DataType,
    pub default_value: Vec<u8>,
    pub comment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub id: u64,
    pub name: String,
    pub table_name: String,
    pub columns: Vec<(String, u64)>,
    pub state: TableState,
    pub primary: bool,
    pub unique: bool,
}

impl TableInfo {
    pub fn get_primary_index(&self) -> Option<Arc<IndexInfo>> {
        for i in self.indices.iter() {
            if i.primary {
                return Some(i.clone());
            }
        }
        None
    }
}
