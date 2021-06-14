mod config;

use super::{Storage, Transaction};
use crate::errors::MySQLResult;
use crate::store::TransactionOptions;
use async_trait::async_trait;
pub use config::TiKVConfig;
use tikv_client::{
    Timestamp, Transaction as KVTransaction, TransactionClient, TransactionOptions as KVTxnOpts,
};

struct TiKVTransaction {
    inner: KVTransaction,
}

pub struct TiKVStorage {
    client: TransactionClient,
}

#[async_trait]
impl Storage for TiKVStorage {
    async fn get(&self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        let opts = KVTxnOpts::new_optimistic();
        let mut snapshot = self.client.snapshot(
            Timestamp {
                physical: 1i64 << 40,
                logical: 0,
                suffix_bits: 0,
            },
            opts,
        );
        let v = snapshot.get(key.to_vec()).await?;
        Ok(v)
    }

    async fn new_transaction(
        &self,
        opts: &TransactionOptions,
    ) -> MySQLResult<Box<dyn Transaction>> {
        let txn = if opts.pessimistic {
            self.client.begin_pessimistic().await?
        } else {
            self.client.begin_optimistic().await?
        };
        Ok(Box::new(TiKVTransaction { inner: txn }))
    }
}

#[async_trait]
impl Transaction for TiKVTransaction {
    async fn commit(&mut self) -> MySQLResult<()> {
        self.inner.commit().await?;
        Ok(())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
        self.inner.put(key.to_vec(), value.to_vec()).await?;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> MySQLResult<()> {
        self.inner.delete(key.to_vec()).await?;
        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
        let v = self.inner.get(key.to_vec()).await?;
        Ok(v)
    }

    async fn scan(&mut self, _start: &[u8], _end: &[u8]) -> MySQLResult<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn get_start_time(&self) -> u64 {
        0
    }
}

impl TiKVStorage {
    pub async fn create(config: &TiKVConfig) -> MySQLResult<TiKVStorage> {
        let client = TransactionClient::new(config.pd_address.clone()).await?;
        Ok(TiKVStorage { client })
    }
}
