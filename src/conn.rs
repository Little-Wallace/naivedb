use super::errors::MySQLError;
use super::server::MysqlServerCore;
use async_trait::async_trait;
use msql_srv::{MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct MysqlConnection {
    core: Arc<MysqlServerCore>,
}

impl MysqlConnection {
    pub fn create(core: Arc<MysqlServerCore>) -> MysqlConnection {
        MysqlConnection { core }
    }
}

#[async_trait]
impl MysqlShim for MysqlConnection {
    type Error = MySQLError;
    async fn on_prepare(
        &mut self,
        query: &str,
        _info: StatementMetaWriter<'_>,
    ) -> Result<(), Self::Error> {
        let dialect = MySqlDialect {};
        let stmts = Parser::parse_sql(&dialect, query)?;
        if stmts.len() != 1 {
            return Err(MySQLError::PrepareMult(stmts.len() as u64));
        }
        Ok(())
    }

    async fn on_execute(
        &mut self,
        _id: u32,
        _params: ParamParser<'_>,
        _results: QueryResultWriter<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn on_close(&mut self, _stmt: u32) {}

    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_>,
    ) -> Result<(), Self::Error> {
        let dialect = MySqlDialect {};
        let stmts = Parser::parse_sql(&dialect, query)?;
        for stmt in stmts {
            println!("stmt: {:?}", stmt);
        }
        results.completed(0, 0).await?;
        Ok(())
    }
}
