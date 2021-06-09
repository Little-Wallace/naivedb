use sqlparser::parser::ParserError;
use std::io;
use thiserror::Error;

pub type MySQLResult<T> = std::result::Result<T, MySQLError>;

/// Describes why a message is discarded.
#[derive(Debug, PartialEq, Clone, Error)]
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

    #[error("table not exist")]
    NoTable,

    #[error("DB not exist")]
    NoDB,

    #[error("unsupported sql")]
    UnsupportSQL,
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
