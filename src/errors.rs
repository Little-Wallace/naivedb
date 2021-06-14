use sqlparser::parser::ParserError;
use std::io;
use thiserror::Error;
use tikv_client::Error as KVError;

pub type MySQLResult<T> = std::result::Result<T, MySQLError>;

/// Describes why a message is discarded.
#[derive(Debug, Error)]
pub enum MySQLError {
    #[error("{0} prepare statement is not allowed")]
    PrepareMult(u64),
    #[error("parse  error : {0}")]
    ParseError(ParserError),

    #[error("io error : {0}")]
    Io(String),

    #[error("primary key has exist")]
    KeyExist,

    #[error("index not exist")]
    NoIndex,

    #[error("column not exist")]
    NoColumn,

    #[error("miss column {0}")]
    MissColumn(String),

    #[error("column not match values")]
    ColumnMissMatch,

    #[error("table not exist")]
    NoTable,

    #[error("DB not exist")]
    NoDB,

    #[error("unsupported sql")]
    UnsupportSQL,

    #[error("TiKV Error")]
    TiKV(KVError),
}

impl From<io::Error> for MySQLError {
    fn from(e: io::Error) -> Self {
        let s = format!("{}", e);
        MySQLError::Io(s)
    }
}

impl From<ParserError> for MySQLError {
    fn from(e: ParserError) -> Self {
        MySQLError::ParseError(e)
    }
}

impl From<std::num::ParseIntError> for MySQLError {
    fn from(e: std::num::ParseIntError) -> Self {
        MySQLError::ParseError(ParserError::ParserError(format!(
            "error when parse int: {:?}",
            e
        )))
    }
}

impl From<std::num::ParseFloatError> for MySQLError {
    fn from(e: std::num::ParseFloatError) -> Self {
        MySQLError::ParseError(ParserError::ParserError(format!(
            "error when parse flot: {:?}",
            e
        )))
    }
}

impl From<KVError> for MySQLError {
    fn from(e: KVError) -> Self {
        MySQLError::TiKV(e)
    }
}
