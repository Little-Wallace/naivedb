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
        let mut result = vec![];
        match value {
            Some(v) => {
                let row = DecoderRow::from_bytes(v)?;
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
        Ok(result)
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
        let mut offsets = vec![values.len(); self.meta.columns.len()];
        for i in 0..vcols.len() {
            offsets[vcols[i].offset] = i;
        }
        let mut default_values = vec![];
        for col in self.meta.columns.iter() {
            let idx = offsets[col.offset];
            if idx >= values.len() {
                if let Some(generator) = col.default_value.as_ref() {
                    offsets[col.offset] = values.len() + default_values.len();
                    default_values.push(generator.generate());
                } else {
                    return Err(MySQLError::MissColumn(format!("Miss column {}", col.name)));
                }
            }
        }

        let key = self.get_record_key(&values, &default_values, &offsets)?;
        if writer.check_constants(&key).await? {
            return Err(MySQLError::KeyExist);
        }

        for col in self.meta.columns.iter() {
            let idx = offsets[col.offset];
            if idx < values.len() {
                row.append_column(col.id as u32, &values[idx], &col.data_type)?;
            } else {
                row.append_column(
                    col.id as u32,
                    &default_values[idx - values.len()],
                    &col.data_type,
                )?;
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
            index_key.clear();
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

    fn get_record_key(
        &self,
        values: &[EncodeValue],
        default_values: &[EncodeValue],
        offsets: &[usize],
    ) -> MySQLResult<Vec<u8>> {
        if let Some(pk_index) = self.meta.get_primary_index() {
            let mut key = Vec::with_capacity(self.get_handle_size());
            key.push(b't');
            key.write_u64::<LittleEndian>(self.id)?;
            key.push(b'r');
            for (_, offset) in pk_index.columns.iter() {
                let col = self.meta.columns[*offset as usize].clone();
                let idx = offsets[col.offset];
                if idx < values.len() {
                    encode_value(&mut key, &values[idx], &col.data_type)?;
                } else {
                    encode_value(
                        &mut key,
                        &default_values[idx - values.len()],
                        &col.data_type,
                    )?;
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::EncodeValue;
    use sqlparser::ast::DataType;
    use tokio::runtime;

    struct TestTransactionContext {
        pub kvs: Vec<(Vec<u8>, Vec<u8>)>,
        pub expected_key: Vec<u8>,
        pub expected_value: Option<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl TransactionContext for TestTransactionContext {
        async fn check_constants(&mut self, _key: &[u8]) -> MySQLResult<bool> {
            Ok(false)
        }

        async fn write(&mut self, key: &[u8], value: &[u8]) -> MySQLResult<()> {
            self.kvs.push((key.to_vec(), value.to_vec()));
            Ok(())
        }

        async fn commit(&mut self) -> MySQLResult<()> {
            Ok(())
        }

        async fn get(&mut self, key: &[u8]) -> MySQLResult<Option<Vec<u8>>> {
            assert_eq!(key.to_vec(), self.expected_key);
            Ok(self.expected_value.clone())
        }
    }

    fn create_table_source() -> TableSource {
        let fields = [
            ("id", DataType::Int),
            ("k", DataType::SmallInt),
            ("name", DataType::String),
            ("t", DataType::Int),
            ("content", DataType::String),
        ];
        let mut id = 0;
        let columns = fields
            .iter()
            .map(|(name, tp)| {
                id += 1;
                let key = if id == 1 {
                    IndexType::Primary
                } else {
                    IndexType::None
                };
                Arc::new(ColumnInfo {
                    id,
                    name: name.to_string(),
                    offset: (id - 1) as usize,
                    data_type: tp.clone(),
                    default_value: None,
                    comment: "".to_string(),
                    key,
                    not_null: false,
                })
            })
            .collect();
        TableSource::new(Arc::new(TableInfo {
            id: 1,
            name: "sbtest".to_string(),
            indices: vec![Arc::new(IndexInfo {
                id: 1,
                name: "".to_string(),
                table_name: "sbtest".to_string(),
                columns: vec![("id".to_string(), 0)],
                state: TableState::Public,
                primary: true,
                unique: false,
            })],
            columns,
            state: TableState::Public,
            pk_is_handle: true,
            max_column_id: 5,
            max_index_id: 1,
            max_row_id: Arc::new(Default::default()),
            update_ts: 0,
        }))
    }

    #[test]
    fn test_insert_and_get_record() {
        let table = create_table_source();
        let mut ctx = TestTransactionContext {
            kvs: vec![],
            expected_key: vec![],
            expected_value: None,
        };
        let mut row = EncoderRow::default();
        let cols = table.meta.columns.clone();
        let values = vec![
            EncodeValue::Int(1),
            EncodeValue::Int(21),
            EncodeValue::Bytes("31".to_string().into_bytes()),
            EncodeValue::Int(41),
            EncodeValue::Bytes("51".to_string().into_bytes()),
        ];
        let mut r = runtime::Runtime::new().unwrap();
        let handle = r
            .block_on(table.add_record(&mut ctx, &mut row, &cols, values.clone()))
            .unwrap();
        assert_eq!(ctx.kvs.len(), 1);
        let value = ctx.kvs[0].1.clone();
        let mut key = vec![];
        key.push(b't');
        key.write_u64::<LittleEndian>(table.id).unwrap();
        key.push(b'r');
        key.extend_from_slice(&handle);
        ctx.expected_value = Some(value.clone());
        ctx.expected_key = key;
        let v = r
            .block_on(table.read_record(
                &mut ctx,
                &DataSchema {
                    columns: cols.clone(),
                },
                &EncodeValue::Int(1),
            ))
            .unwrap();
        assert_eq!(v, values);
        let v = r
            .block_on(table.read_record(
                &mut ctx,
                &DataSchema {
                    columns: vec![cols[1].clone(), cols[4].clone(), cols[2].clone()],
                },
                &EncodeValue::Int(1),
            ))
            .unwrap();
        assert_eq!(
            v,
            vec![values[1].clone(), values[4].clone(), values[2].clone()]
        );
    }
}
