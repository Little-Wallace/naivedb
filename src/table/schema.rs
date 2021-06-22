use crate::common::EncodeValue;
use crate::errors::{MySQLError, MySQLResult};
use msql_srv::{Column, ColumnFlags, ColumnType};
use sqlparser::ast::DataType;
use sqlparser::ast::{ColumnDef, ColumnOption, Expr, Ident, ObjectName, TableConstraint, Value};
use sqlparser::dialect::keywords;
use sqlparser::tokenizer::{Token, Word};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait ValueGenerator: Send + Sync + Debug {
    fn generate(&self) -> EncodeValue;
    fn name(&self) -> &str;
    fn clone_box(&self) -> Box<dyn ValueGenerator>;
}

#[derive(Debug, Clone)]
pub struct DefaultValueGenerator {
    default_value: EncodeValue,
}

impl DefaultValueGenerator {
    pub fn new(default_value: EncodeValue) -> Self {
        DefaultValueGenerator { default_value }
    }
}

impl ValueGenerator for DefaultValueGenerator {
    fn generate(&self) -> EncodeValue {
        self.default_value.clone()
    }

    fn name(&self) -> &str {
        "DefaultValueGenerator"
    }

    fn clone_box(&self) -> Box<dyn ValueGenerator> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct AutoIncrementIdGenerator {
    max_id: Arc<AtomicU64>,
}

impl AutoIncrementIdGenerator {
    pub fn new(max_id: Arc<AtomicU64>) -> Self {
        AutoIncrementIdGenerator { max_id }
    }
}

impl ValueGenerator for AutoIncrementIdGenerator {
    fn generate(&self) -> EncodeValue {
        let id = self.max_id.fetch_add(1, Ordering::SeqCst);
        EncodeValue::Int(id as i64)
    }

    fn name(&self) -> &str {
        "AutoIncrementIdGenerator"
    }

    fn clone_box(&self) -> Box<dyn ValueGenerator> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableState {
    Tombstone,
    Public,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub id: u64,
    pub name: String,
    pub columns: Vec<Arc<ColumnInfo>>,
    pub indices: Vec<Arc<IndexInfo>>,
    pub state: TableState,
    pub pk_is_handle: bool,
    pub max_column_id: u64,
    pub max_index_id: u64,
    pub max_row_id: Arc<AtomicU64>,
    pub update_ts: u64,
}

#[derive(Debug)]
pub struct ColumnInfo {
    pub id: u64,
    pub name: String,
    pub offset: usize,
    pub data_type: DataType,
    pub default_value: Option<Box<dyn ValueGenerator>>,
    pub comment: String,
    pub key: IndexType,
    pub not_null: bool,
}

impl Clone for ColumnInfo {
    fn clone(&self) -> Self {
        ColumnInfo {
            id: self.id,
            name: self.name.clone(),
            offset: self.offset,
            data_type: self.data_type.clone(),
            default_value: self.default_value.as_ref().map(|v| v.clone_box()),
            comment: self.comment.clone(),
            key: self.key,
            not_null: self.not_null,
        }
    }
}

impl PartialEq for ColumnInfo {
    fn eq(&self, other: &Self) -> bool {
        let eq = self.id == other.id
            && self.name == other.name
            && self.offset == other.offset
            && self.comment == other.comment
            && self.key == other.key
            && self.not_null == other.not_null;
        if !eq {
            return false;
        }
        if let Some(f) = self.default_value.as_ref() {
            return other
                .default_value
                .as_ref()
                .map_or(false, |v| v.name() == f.name());
        }
        other.default_value.is_none()
    }
}

impl Eq for ColumnInfo {}

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
            max_column_id: 0,
            max_index_id: 0,
            update_ts: 0,
            max_row_id: Arc::new(AtomicU64::new(0)),
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
            default_value: None,
            comment: "".to_string(),
            key: IndexType::None,
            not_null: false,
        };
        for opt in col_def.options.iter() {
            match &opt.option {
                ColumnOption::Unique { is_primary } => {
                    col.key = IndexType::Primary;
                    constraints.push(TableConstraint::Unique {
                        name: None,
                        columns: vec![Ident {
                            value: col_def.name.value.to_lowercase(),
                            quote_style: None,
                        }],
                        is_primary: *is_primary,
                    });
                }
                ColumnOption::NotNull => {
                    col.not_null = true;
                }
                ColumnOption::DialectSpecific(others) => {
                    for word in others {
                        if let Token::Word(Word { value, keyword, .. }) = word {
                            if value.to_lowercase() == "auto_increment"
                                && *keyword == keywords::Keyword::AUTO_INCREMENT
                            {
                                col.default_value = Some(Box::new(AutoIncrementIdGenerator::new(
                                    self.max_row_id.clone(),
                                )));
                            }
                        }
                    }
                }
                ColumnOption::Default(expr) => {
                    if let Expr::Value(data) = expr {
                        let val = match data {
                            Value::SingleQuotedString(val) => {
                                self.parse_column_string_value(&col.data_type, val)?
                            }
                            Value::DoubleQuotedString(val) => {
                                self.parse_column_string_value(&col.data_type, val)?
                            }
                            d => EncodeValue::from_parse_value(d.clone())?,
                        };
                        col.default_value = Some(Box::new(DefaultValueGenerator::new(val)));
                    }
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
                is_primary,
            } => {
                if let Some(name) = name {
                    index_info.name = name.value.to_lowercase();
                }
                index_info.primary = is_primary;
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

    fn parse_column_string_value(
        &self,
        data_type: &DataType,
        val: &String,
    ) -> MySQLResult<EncodeValue> {
        let v = match data_type {
            DataType::Int | DataType::BigInt | DataType::SmallInt => {
                let v = val.parse::<i64>()?;
                EncodeValue::Int(v)
            }
            DataType::Double => {
                let v = val.parse::<f64>()?;
                EncodeValue::Double(v)
            }
            DataType::String => EncodeValue::Bytes(val.as_bytes().to_vec()),
            DataType::Char(_) => EncodeValue::Bytes(val.as_bytes().to_vec()),
            _ => return Err(MySQLError::ColumnMissMatch),
        };
        Ok(v)
    }
}

impl ColumnInfo {
    pub fn to_mysql_column(&self) -> MySQLResult<Column> {
        let tp = match &self.data_type {
            DataType::Char(_) => ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::Varchar(_) => ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::Decimal(_, _) => ColumnType::MYSQL_TYPE_DECIMAL,
            DataType::Float(_) => ColumnType::MYSQL_TYPE_FLOAT,
            DataType::SmallInt => ColumnType::MYSQL_TYPE_SHORT,
            DataType::Int => ColumnType::MYSQL_TYPE_LONG,
            DataType::BigInt => ColumnType::MYSQL_TYPE_LONG,
            DataType::Double => ColumnType::MYSQL_TYPE_FLOAT,
            DataType::Boolean => ColumnType::MYSQL_TYPE_SHORT,
            DataType::Text => ColumnType::MYSQL_TYPE_VARCHAR,
            DataType::String => ColumnType::MYSQL_TYPE_VAR_STRING,
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
