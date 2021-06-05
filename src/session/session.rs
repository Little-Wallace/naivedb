use crate::errors::{MySQLError, MySQLResult};
use crate::table::table::TableSource;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct Session {
    cache: HashMap<String, Arc<TableSource>>,
    tables: Arc<RwLock<HashMap<String, Arc<TableSource>>>>,
    db: String,
}

pub type SessionRef = Arc<Mutex<Session>>;

impl Session {
    pub fn new(tables: Arc<RwLock<HashMap<String, Arc<TableSource>>>>) -> Session {
        Session {
            tables,
            cache: HashMap::default(),
            db: "".to_string(),
        }
    }

    pub fn get_table(&mut self, name: &String) -> Option<Arc<TableSource>> {
        if let Some(table) = self.cache.get(name) {
            return Some(table.clone());
        }
        let table = self.tables.read().unwrap().get(name).map(|t|t.clone());
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
}
