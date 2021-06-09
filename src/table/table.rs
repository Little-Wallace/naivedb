use super::schema::*;
use crate::common::EncodeValue;
use crate::errors::MySQLError;
use crate::errors::MySQLResult;
use crate::table::decoder::{encode_value, get_handle_from_record_key, EncoderRow};
use crate::transaction::TransactionContext;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sqlparser::ast::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TableSource {
    id: u64,
    meta: Arc<TableInfo>,
    column_map: HashMap<String, Arc<ColumnInfo>>,
    unique_index_map: HashMap<String, Arc<IndexInfo>>,
}

impl TableSource {
    pub fn new(table: Arc<TableInfo>) -> TableSource {
        let mut column_map = HashMap::default();
        let mut unique_index_map = HashMap::default();
        for c in table.columns.iter() {
            column_map.insert(c.name.clone(), c.clone());
        }
        for i in table.indices.iter() {
            if (i.unique || i.primary) && i.columns.len() == 1 {
                unique_index_map.insert(i.columns.first().unwrap().0.clone(), i.clone());
            }
        }
        TableSource {
            id: table.id,
            meta: table,
            column_map,
            unique_index_map,
        }
    }

    pub fn get_column(&self, name: &String) -> Option<Arc<ColumnInfo>> {
        self.column_map.get(name).map(|col| col.clone())
    }

    pub fn get_index(&self, name: &String) -> Option<Arc<IndexInfo>> {
        self.unique_index_map.get(name).map(|col| col.clone())
    }

    pub async fn read_record<W: TransactionContext>(
        &self,
        reader: &mut W,
        select_cols: &DataSchema,
        primary: &EncodeValue,
    ) -> MySQLResult<Vec<EncodeValue>> {
        Ok(vec![])
    }

    pub async fn read_record_by_index<W: TransactionContext>(
        &self,
        reader: &mut W,
        primary_info: &IndexInfo,
        select_cols: &DataSchema,
        primary: &EncodeValue,
    ) -> MySQLResult<Vec<EncodeValue>> {
        Ok(vec![])
    }

    pub async fn read_handle_from_index<W: TransactionContext>(
        &self,
        reader: &mut W,
        index_info: &IndexInfo,
        index: &EncodeValue,
    ) -> MySQLResult<Option<EncodeValue>> {
        Ok(None)
    }

    pub async fn add_record<W: TransactionContext>(
        &self,
        writer: &mut W,
        row: &mut EncoderRow,
        values: Vec<EncodeValue>,
    ) -> MySQLResult<Vec<u8>> {
        let key = self.get_record_key(&values)?;
        if writer.check_constants(&key).await? {
            return Err(MySQLError::KeyExist);
        }
        for i in 0..self.meta.columns.len() {
            row.append_column(
                self.meta.columns[i].id as u32,
                &values[i],
                &self.meta.columns[i].data_type,
            )?;
        }
        let handle = get_handle_from_record_key(&key);

        let mut index_key = Vec::with_capacity(self.get_handle_size());
        for index in self.meta.indices.iter() {
            if index.primary {
                continue;
            }
            index_key.clear();
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

    fn get_record_key(&self, values: &[EncodeValue]) -> MySQLResult<Vec<u8>> {
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
