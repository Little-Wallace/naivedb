use crate::errors::{MySQLError, MySQLResult};
use sqlparser::ast::DataType;
use sqlparser::ast::{ColumnDef, ColumnOption, Ident, ObjectName, TableConstraint};
use msql_srv::{Column, ColumnType, ColumnFlags};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableState {
    Tombstone,
    Public,
    WriteOnly,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    None,
    Primary,
    MultipleUnqiue,
    Unique,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSchema {
    pub columns: Vec<Arc<ColumnInfo>>,
}

pub type DataSchemaRef = Arc<DataSchema>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub id: u64,
    pub name: String,
    pub columns: Vec<Arc<ColumnInfo>>,
    pub indices: Vec<Arc<IndexInfo>>,
    pub state: TableState,
    pub pk_is_handle: bool,
    pub auto_inc_id: u64,
    pub max_column_id: u64,
    pub max_index_id: u64,
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
    pub key: IndexType,
    pub not_null: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub id: u64,
    pub name: String,
    pub table_name: String,
    pub columns: Vec<(String, usize)>,
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

    pub fn create(
        name: &ObjectName,
        column_defs: &Vec<ColumnDef>,
        constrains: &Vec<TableConstraint>,
    ) -> MySQLResult<TableInfo> {
        if name.0.is_empty() {
            return Err(MySQLError::UnsupportSQL);
        }

        let table_name = name.0.last().unwrap().value.to_lowercase();
        let mut table_info = TableInfo {
            id: 0,
            name: table_name,
            columns: vec![],
            indices: vec![],
            state: TableState::Public,
            pk_is_handle: true,
            auto_inc_id: 0,
            max_column_id: 0,
            max_index_id: 0,
            update_ts: 0,
        };
        table_info.build_columns_and_constraints(column_defs, constrains)?;
        Ok(table_info)
    }

    pub fn build_columns_and_constraints(
        &mut self,
        column_defs: &Vec<ColumnDef>,
        constrains: &Vec<TableConstraint>,
    ) -> MySQLResult<()> {
        let mut constraints = constrains.clone();
        let mut offset = 0;
        let mut cols = vec![];
        for col_def in column_defs {
            let column = self.build_column(&mut constraints, offset, col_def)?;
            cols.push(column);
            offset += 1;
        }
        for c in constraints.iter_mut() {
            match c {
                TableConstraint::Unique {
                    is_primary,
                    columns,
                    ..
                } => {
                    for c in columns {
                        c.value = c.value.to_lowercase();
                        let l = cols.len();
                        for col in cols.iter_mut() {
                            if col.name == c.value {
                                if *is_primary {
                                    col.key = IndexType::Primary;
                                } else if l > 0 {
                                    col.key = IndexType::MultipleUnqiue;
                                } else {
                                    col.key = IndexType::Unique;
                                }
                            }
                        }
                    }
                }
                _ => return Err(MySQLError::UnsupportSQL),
            }
        }
        for mut col in cols {
            self.max_column_id += 1;
            col.id = self.max_column_id;
            self.columns.push(Arc::new(col));
        }
        for constriant in constraints {
            let mut index_info = self.build_index_info(constriant)?;
            self.max_index_id += 1;
            index_info.id = self.max_index_id;
            self.indices.push(Arc::new(index_info));
        }
        // TODO: Check constraints conflict and valid.
        Ok(())
    }

    fn build_column(
        &self,
        constraints: &mut Vec<TableConstraint>,
        offset: usize,
        col_def: &ColumnDef,
    ) -> MySQLResult<ColumnInfo> {
        let mut col = ColumnInfo {
            id: 0,
            name: col_def.name.value.to_lowercase(),
            offset,
            data_type: col_def.data_type.clone(),
            default_value: vec![],
            comment: "".to_string(),
            key: IndexType::None,
            not_null: false,
        };
        for opt in col_def.options.iter() {
            match opt.option {
                ColumnOption::Unique { is_primary } => {
                    col.key = IndexType::Primary;
                    constraints.push(TableConstraint::Unique {
                        name: None,
                        columns: vec![Ident {
                            value: col_def.name.value.to_lowercase(),
                            quote_style: None,
                        }],
                        is_primary,
                    });
                }
                ColumnOption::NotNull => {
                    col.not_null = true;
                }
                _ => {
                    // TODO: support unique constraint in column define
                }
            }
        }
        Ok(col)
    }

    fn build_index_info(&self, constraint: TableConstraint) -> MySQLResult<IndexInfo> {
        let mut index_info = IndexInfo {
            id: 0,
            name: "".to_string(),
            table_name: self.name.clone(),
            columns: vec![],
            state: TableState::Tombstone,
            primary: false,
            unique: false,
        };
        match constraint {
            TableConstraint::Unique {
                columns,
                name,
                ..
            } => {
                if let Some(name) = name {
                    index_info.name = name.value.to_lowercase();
                }
                for key in columns {
                    let mut index_col = self.columns[0].clone();
                    for col in self.columns.iter() {
                        if col.name == key.value {
                            index_col = col.clone();
                            break;
                        }
                    }
                    if index_col.name != key.value {
                        return Err(MySQLError::NoColumn);
                    }
                    index_info
                        .columns
                        .push((index_col.name.clone(), index_col.offset));
                }
            }
            _ => (),
        }
        Ok(index_info)
    }
}


impl ColumnInfo {
    pub fn to_mysql_column(&self) -> MySQLResult<Column> {
        let tp = match &self.data_type {
            DataType::Char(size) =>
                ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::Varchar(size) =>
                ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::Decimal(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::Float(size) => ColumnType::MYSQL_TYPE_FLOAT,
            DataType::SmallInt => ColumnType::MYSQL_TYPE_SHORT,
            DataType::Int => ColumnType::MYSQL_TYPE_LONG,
            DataType::BigInt => ColumnType::MYSQL_TYPE_LONG,
            DataType::Double =>
                ColumnType::MYSQL_TYPE_FLOAT,
            DataType::Boolean => ColumnType::MYSQL_TYPE_SHORT,
            DataType::Text =>
                ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::String =>
                ColumnType::MYSQL_TYPE_VAR_STRING,
            _ => return Err(MySQLError::UnsupportSQL),
        };
        Ok(Column {
            table: "".to_string(),
            column: self.name.clone(),
            coltype: tp,
            colflags: ColumnFlags::empty(),
        })
    }
}