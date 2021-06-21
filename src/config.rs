use crate::store::TiKVConfig;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageType {
    Mem,
    TiKV,
    Local,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub tikv: TiKVConfig,
    pub storage: StorageType,
    pub connection_poo_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            tikv: TiKVConfig::default(),
            storage: StorageType::Mem,
            connection_poo_size: 4,
        }
    }
}

impl Config {
    pub fn from_file(path: &Path) -> Config {
        let s = std::fs::read_to_string(path).unwrap();
        let mut deserializer = toml::Deserializer::new(&s);
        let cfg = <Config as serde::Deserialize>::deserialize(&mut deserializer).unwrap();
        cfg
    }
}
