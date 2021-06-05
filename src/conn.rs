use super::errors::MySQLError;
use crate::session::{Session, SessionRef};
use crate::table::table::TableSource;
use async_trait::async_trait;
use msql_srv::{
    ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use crate::planner::PlanBuilder;

pub struct MysqlServerCore {
    tables: Arc<RwLock<HashMap<String, Arc<TableSource>>>>,
}

impl MysqlServerCore {

    pub fn new() -> MysqlServerCore {
        // TODO: load schema from storage.
        let tables = Arc::new(RwLock::new(HashMap::default()));
        MysqlServerCore { tables }
    }

    pub fn create_connection(&self) -> MysqlConnection {
        MysqlConnection::new(Session::new(self.tables.clone()))
    }
}

pub struct MysqlConnection {
    session: SessionRef,
}

impl MysqlConnection {
    pub fn new(session: Session) -> MysqlConnection {
        MysqlConnection {
            session: SessionRef::new(Mutex::new(session)),
        }
    }
}

#[async_trait]
impl MysqlShim for MysqlConnection {
    type Error = MySQLError;
    /// Called when client switches database.
    async fn on_init(&mut self, db: &str, w: InitWriter<'_>) -> Result<(), Self::Error> {
        let e = {
            let mut guard = self.session.lock().unwrap();
            match guard.set_db(db.to_string()) {
            Ok(_) => return w.ok().map_err(|e| e.into()),
            Err(e) => e,
            }
        };
        w.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes()).await?;
        Ok(())
    }

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
        let plan_builder = PlanBuilder::create(self.session.clone());
        let plan = plan_builder.build_from_sql(query)?;
        results.completed(0, 0).await?;
        Ok(())
    }
}
