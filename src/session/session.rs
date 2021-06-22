use crate::errors::MySQLResult;
use crate::store::Transaction;
use crate::table::schema::TableInfo;
use crate::table::table::TableSource;
use crate::table::DBTableManager;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct Session {
    cache: HashMap<String, Arc<TableSource>>,
    table_mgr: Arc<RwLock<DBTableManager>>,
    db: String,
    transaction: Option<Box<dyn Transaction>>,
    pub is_in_txn: bool,
}

pub type SessionRef = Arc<Mutex<Session>>;

impl Session {
    pub fn new(table_mgr: Arc<RwLock<DBTableManager>>) -> Session {
        Session {
            table_mgr,
            cache: HashMap::default(),
            db: "".to_string(),
            transaction: None,
            is_in_txn: false,
        }
    }

    pub fn take_transaction(&mut self) -> Option<Box<dyn Transaction>> {
        self.transaction.take()
    }

    pub fn set_transaction(&mut self, txn: Box<dyn Transaction>) {
        self.transaction = Some(txn)
    }

    pub fn add_table(&mut self, name: String, table_info: TableInfo) {
        let mut tables = self.table_mgr.write().unwrap();
        let table = tables.add_table(name.clone(), table_info);
        self.cache.insert(name, table);
    }

    pub fn replace_table(&mut self, name: String, table_info: TableInfo) {
        let mut tables = self.table_mgr.write().unwrap();
        let table = tables.replace_table(name.clone(), table_info);
        self.cache.insert(name, table);
    }

    pub fn get_table(&mut self, name: &String) -> Option<Arc<TableSource>> {
        if let Some(table) = self.cache.get(name) {
            if table.is_valid() {
                return Some(table.clone());
            }
        }
        let table = self.table_mgr.read().unwrap().get_table(name);
        if let Some(t) = table.as_ref() {
            self.cache.insert(name.clone(), t.clone());
        }
        table
    }

    pub fn set_db(&mut self, name: String) -> MySQLResult<()> {
        self.db = name;
        // TODO: get db info
        Ok(())
    }

    pub fn get_db(&self) -> &String {
        &self.db
    }
}
