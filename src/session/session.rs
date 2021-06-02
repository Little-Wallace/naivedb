use crate::table::table::TableSource;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Session {
    cache: HashMap<String, Arc<TableSource>>,
    table: Arc<Mutex<HashMap<String, Arc<TableSource>>>>,
}
