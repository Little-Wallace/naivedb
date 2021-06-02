use super::schema::*;
use crate::common::EncodeValue;
use crate::errors::MySQLError;
use crate::errors::MysqlResult;
use crate::table::decoder::{encode_value, get_handle_from_record_key, EncoderRow};
use async_trait::async_trait;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sqlparser::ast::Value;
use std::sync::Arc;

#[async_trait]
pub trait TransactionWriter {
    async fn check_constants(&self, key: &[u8]) -> MysqlResult<bool>;
    async fn write(&mut self, key: &[u8], value: &[u8]) -> MysqlResult<()>;
    async fn commit(&mut self) -> MysqlResult<()>;
}

pub struct TableSource {
    id: u64,
    meta: TableInfo,
    columns: Vec<Arc<ColumnInfo>>,
    indices: Vec<Arc<IndexInfo>>,
}

impl TableSource {
    pub async fn add_record<W: TransactionWriter>(
        &self,
        writer: &mut W,
        row: &mut EncoderRow,
        values: Vec<EncodeValue>,
    ) -> MysqlResult<Vec<u8>> {
        let key = self.get_record_key(&values)?;
        if writer.check_constants(&key).await? {
            return Err(MySQLError::KeyExist);
        }
        for i in 0..self.columns.len() {
            row.append_columen(
                self.columns[i].id as u32,
                &values[i],
                &self.columns[i].data_type,
            )?;
        }
        let handle = get_handle_from_record_key(&key);

        for index in self.indices.iter() {
            if index.primary {
                continue;
            }
            let mut index_key = Vec::with_capacity(self.get_handle_size());
            index_key.push(b't');
            index_key.write_u64::<LittleEndian>(self.id)?;
            index_key.push(b'i');
            for (_, offset) in index.columns.iter() {
                let col = self.meta.columns[*offset as usize].clone();
                encode_value(&mut index_key, &values[col.offset], &col.data_type)?;
            }
            writer.write(&index_key, handle).await?;
        }

        let value = row.to_bytes()?;
        row.clear();
        writer.write(&key, &value).await?;
        Ok(handle.to_vec())
    }

    fn get_record_key(&self, values: &[EncodeValue]) -> MysqlResult<Vec<u8>> {
        if let Some(pk_index) = self.meta.get_primary_index() {
            let mut key = Vec::with_capacity(self.get_handle_size());
            key.push(b't');
            key.write_u64::<LittleEndian>(self.id)?;
            key.push(b'r');
            for (_, offset) in pk_index.columns.iter() {
                let col = self.meta.columns[*offset as usize].clone();
                encode_value(&mut key, &values[col.offset], &col.data_type)?;
            }
            return Ok(key);
        }
        Err(MySQLError::NoIndex)
    }

    fn get_handle_size(&self) -> usize {
        64
    }
}
