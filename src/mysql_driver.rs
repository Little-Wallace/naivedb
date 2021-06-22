use super::errors::{MySQLError, MySQLResult};
use crate::common::SendableDataBlockStream;
use crate::config::{Config, StorageType};
use crate::executor::ExecutorBuilder;
use crate::planner::PlanBuilder;
use crate::session::{Session, SessionRef};
use crate::store::Storage;
use crate::store::{MemStorage, TiKVStorage};
use crate::table::DBTableManager;
use async_trait::async_trait;
use msql_srv::{
    ErrorKind, InitWriter, MysqlShim, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::{Arc, Mutex, RwLock};

pub struct MysqlServerCore {
    table_mgr: Arc<RwLock<DBTableManager>>,
    storage: Arc<dyn Storage>,
}

impl Default for MysqlServerCore {
    fn default() -> MysqlServerCore {
        let table_mgr = Arc::new(RwLock::new(DBTableManager::new()));
        MysqlServerCore {
            table_mgr,
            storage: Arc::new(MemStorage::new()),
        }
    }
}

impl MysqlServerCore {
    pub async fn new(config: Config) -> MysqlServerCore {
        let storage: Arc<dyn Storage> = match config.storage {
            StorageType::Tikv => Arc::new(TiKVStorage::create(&config.tikv).await.unwrap()),
            StorageType::Mem => Arc::new(MemStorage::new()),
            _ => panic!("unsupport storage type"),
        };
        let table_mgr = Arc::new(RwLock::new(DBTableManager::new()));
        MysqlServerCore { table_mgr, storage }
    }

    pub fn create_connection(&self) -> ConnectionDriver {
        ConnectionDriver::new(Session::new(self.table_mgr.clone()), self.storage.clone())
    }
}

pub struct ConnectionDriver {
    session: SessionRef,
    storage: Arc<dyn Storage>,
}

impl ConnectionDriver {
    pub fn new(session: Session, storage: Arc<dyn Storage>) -> ConnectionDriver {
        ConnectionDriver {
            session: SessionRef::new(Mutex::new(session)),
            storage,
        }
    }
    pub fn get_session(&self) -> SessionRef {
        self.session.clone()
    }
}

#[async_trait]
impl MysqlShim for ConnectionDriver {
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
        w.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())
            .await?;
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
        let output = async move {
            let plan = plan_builder.build_from_sql(query)?;
            let mut executor =
                ExecutorBuilder::build(plan, self.session.clone(), self.storage.clone());
            executor.execute().await
        };

        match output.await {
            Ok(data) => {
                done(data, results).await?;
            }
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())
                    .await?;
            }
        }
        Ok(())
    }
}

async fn done<'a>(rows: SendableDataBlockStream, writer: QueryResultWriter<'a>) -> MySQLResult<()> {
    if rows.is_empty() {
        writer.completed(0, 0).await?;
        return Ok(());
    }
    let mut cols = vec![];
    for c in rows[0].schema.columns.iter() {
        cols.push(c.to_mysql_column()?);
    }
    let mut row_writer = writer.start(&cols).await?;
    for block in rows {
        for row in block.data {
            // let data = Vec::with_capacity(row.into_iter().map(||))
            row_writer.write_row(row.into_iter().map(|v| String::from(v)))?;
        }
    }
    row_writer.finish().await?;
    Ok(())
}
