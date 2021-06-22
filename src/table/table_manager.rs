use crate::table::schema::TableInfo;
use crate::table::table::TableSource;
use std::collections::HashMap;
use std::sync::Arc;

pub struct DBTableManager {
    tables: HashMap<String, Arc<TableSource>>,
    max_table_id: u64,
}

impl DBTableManager {
    pub fn new() -> DBTableManager {
        DBTableManager {
            max_table_id: 0,
            tables: HashMap::default(),
        }
    }

    pub fn add_table(&mut self, name: String, mut table_info: TableInfo) -> Arc<TableSource> {
        self.max_table_id += 1;
        table_info.id = self.max_table_id;
        let table = Arc::new(TableSource::new(Arc::new(table_info)));
        self.tables.insert(name.clone(), table.clone());
        table
    }
    pub fn replace_table(&mut self, name: String, table_info: TableInfo) -> Arc<TableSource> {
        let table = Arc::new(TableSource::new(Arc::new(table_info)));
        if let Some(t) = self.tables.insert(name.clone(), table.clone()) {
            t.invalid();
        }
        table
    }

    pub fn get_table(&self, name: &String) -> Option<Arc<TableSource>> {
        self.tables.get(name).map(|t| t.clone())
    }
}
