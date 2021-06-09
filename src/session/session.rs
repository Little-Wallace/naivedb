use crate::errors::{MySQLError, MySQLResult};
use crate::store::Transaction;
use crate::table::table::TableSource;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct Session {
    cache: HashMap<String, Arc<TableSource>>,
    tables: Arc<RwLock<HashMap<String, Arc<TableSource>>>>,
    db: String,
    transaction: Option<Box<dyn Transaction>>,
    pub is_in_txn: bool,
}

pub type SessionRef = Arc<Mutex<Session>>;

impl Session {
    pub fn new(tables: Arc<RwLock<HashMap<String, Arc<TableSource>>>>) -> Session {
        Session {
            tables,
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

    pub fn add_table(&mut self, name: String, table: Arc<TableSource>) {
        let mut tables = self.tables.write().unwrap();
        tables.insert(name, table);
    }

    pub fn get_table(&mut self, name: &String) -> Option<Arc<TableSource>> {
        if let Some(table) = self.cache.get(name) {
            return Some(table.clone());
        }
        let table = self.tables.read().unwrap().get(name).map(|t| t.clone());
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
