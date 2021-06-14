use serde::{Deserialize, Serialize};
const DEFAULT_GRPC_CONNECT_TIMEOUT: usize = 4 * 60 * 1000; // 4min
const DEFAULT_GRPC_POOL_SIZE: usize = 4;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKVConfig {
    /// Address of pd-server.
    pub pd_address: Vec<String>,

    /// The size of grpc client pool which connect to pd and tikv
    pub grpc_pool_size: usize,

    /// The minimal unit is ms
    pub grpc_connect_timeout: usize,
}

impl Default for TiKVConfig {
    fn default() -> TiKVConfig {
        TiKVConfig {
            pd_address: vec!["127.0.0.1:2379".to_string()],
            grpc_pool_size: DEFAULT_GRPC_POOL_SIZE,
            grpc_connect_timeout: DEFAULT_GRPC_CONNECT_TIMEOUT,
        }
    }
}
