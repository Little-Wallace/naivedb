use super::schema::*;
use crate::common::EncodeValue;
use crate::errors::MySQLError;
use crate::errors::MySQLResult;
use crate::table::decoder::{encode_value, get_handle_from_record_key, DecoderRow, EncoderRow};
use crate::transaction::TransactionContext;
use byteorder::{LittleEndian, WriteBytesExt};
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
        handle: &EncodeValue,
    ) -> MySQLResult<Vec<EncodeValue>> {
        if let Some(primary_info) = self.meta.get_primary_index() {
            return self
                .read_record_by_index(reader, primary_info.as_ref(), select_cols, handle)
                .await;
        }
        Err(MySQLError::NoIndex)
    }

    pub async fn read_record_by_index<W: TransactionContext>(
        &self,
        reader: &mut W,
        primary_info: &IndexInfo,
        select_cols: &DataSchema,
        handle: &EncodeValue,
    ) -> MySQLResult<Vec<EncodeValue>> {
        let key = self.get_record_by_handle(primary_info, handle)?;
        let value = reader.get(&key).await?;
        match value {
            Some(v) => {
                let row = DecoderRow::from_bytes(v)?;
                let mut result = vec![];
                for col in select_cols.columns.iter() {
                    match row.get_data(col.id as u32) {
                        Some(Some(mut v)) => {
                            result.push(EncodeValue::read_from(&mut v, &col.data_type)?);
                        }
                        _ => result.push(EncodeValue::NULL),
                    }
                }
            }
            None => {
                return Ok(vec![]);
            }
        }
        Ok(vec![])
    }

    pub async fn read_handle_from_index<W: TransactionContext>(
        &self,
        reader: &mut W,
        index_info: &IndexInfo,
        index: &EncodeValue,
    ) -> MySQLResult<Option<EncodeValue>> {
        let mut index_key = Vec::with_capacity(self.get_handle_size());
        self.encode_index_key(&mut index_key, index_info, &[index.clone()])?;
        match reader.get(&index_key).await? {
            None => Ok(None),
            Some(v) => {
                let col = self.meta.columns[index_info.columns[0].1].as_ref();
                Ok(Some(EncodeValue::read_from(
                    &mut v.as_ref(),
                    &col.data_type,
                )?))
            }
        }
    }

    pub async fn add_record<W: TransactionContext>(
        &self,
        writer: &mut W,
        row: &mut EncoderRow,
        vcols: &[Arc<ColumnInfo>],
        values: Vec<EncodeValue>,
    ) -> MySQLResult<Vec<u8>> {
        let key = self.get_record_key(&values)?;
        if writer.check_constants(&key).await? {
            return Err(MySQLError::KeyExist);
        }
        let mut offsets = vec![self.meta.columns.len(), self.meta.columns.len()];
        for i in 0..vcols.len() {
            offsets[vcols[i].offset] = i;
        }
        for col in self.meta.columns.iter() {
            let idx = offsets[col.offset];
            if idx < values.len() {
                row.append_column(col.id as u32, &values[idx], &col.data_type)?;
            } else {
                if let Some(generator) = col.default_value.as_ref() {
                    row.append_column(col.id as u32, &generator.generate(), &col.data_type)?;
                } else {
                    return Err(MySQLError::MissColumn(format!("Miss column {}", col.name)));
                }
            }
        }

        let handle = get_handle_from_record_key(&key);

        let mut index_key = Vec::with_capacity(self.get_handle_size());
        for index in self.meta.indices.iter() {
            if index.primary {
                continue;
            }
            self.encode_index_key(&mut index_key, index.as_ref(), &values)?;
            writer.write(&index_key, handle).await?;
        }

        let value = row.to_bytes()?;
        writer.write(&key, value).await?;
        Ok(handle.to_vec())
    }

    fn get_record_by_handle(&self, info: &IndexInfo, handle: &EncodeValue) -> MySQLResult<Vec<u8>> {
        let mut key = Vec::with_capacity(self.get_handle_size());
        key.push(b't');
        key.write_u64::<LittleEndian>(self.id)?;
        key.push(b'r');
        let col = self.meta.columns[info.columns[0].1].as_ref();
        encode_value(&mut key, handle, &col.data_type)?;
        Ok(key)
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

    fn encode_index_key(
        &self,
        index_key: &mut Vec<u8>,
        index_info: &IndexInfo,
        values: &[EncodeValue],
    ) -> MySQLResult<()> {
        index_key.clear();
        index_key.push(b't');
        index_key.write_u64::<LittleEndian>(self.id)?;
        index_key.push(b'i');
        for (_, offset) in index_info.columns.iter() {
            let col = self.meta.columns[*offset as usize].clone();
            encode_value(index_key, &values[col.offset], &col.data_type)?;
        }
        Ok(())
    }

    fn get_handle_size(&self) -> usize {
        64
    }
}
