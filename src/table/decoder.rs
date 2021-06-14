use crate::common::EncodeValue;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sqlparser::ast::DataType;
use std::io;

const ROW_HEADER: &[u8] = &[128, 0];

#[derive(Default, Clone)]
pub struct DecoderRow {
    pub cols: Vec<u32>,
    pub offsets: Vec<usize>,
    pub data: Vec<u8>,
    pub cursor: usize,
    pub num_not_null_cols: usize,
}

impl DecoderRow {
    pub fn from_bytes(data: Vec<u8>) -> io::Result<DecoderRow> {
        // store flag and version in data[0..2]
        let mut input = data.as_slice();
        let _ = input.read_u8()?;
        let _ = input.read_u8()?;
        let num_not_null_cols = input.read_u16::<LittleEndian>()? as usize;
        let num_null_cols = input.read_u16::<LittleEndian>()? as usize;
        let mut cursor = 6 as usize;
        let col_ids_len = num_not_null_cols + num_null_cols;
        let mut cols = vec![];
        let mut offsets = vec![];
        for _ in 0..col_ids_len {
            cols.push(input.read_u32::<LittleEndian>()?);
        }
        cursor += col_ids_len * 4;
        for _ in 0..num_not_null_cols {
            offsets.push(input.read_u32::<LittleEndian>()? as usize);
        }
        print!("cols: [");
        for i in cols.iter() {
            print!("{},", *i);
        }
        println!("]");
        print!("offsets: [");
        for i in offsets.iter() {
            print!("{},", *i);
        }
        println!("]");
        println!("cursor: {}", cursor);

        cursor += num_not_null_cols * 4;

        Ok(DecoderRow {
            data,
            cursor,
            cols,
            offsets,
            num_not_null_cols,
        })
    }

    pub fn get_data(&self, i: u32) -> Option<Option<&[u8]>> {
        let x = self.cols[0..self.num_not_null_cols].binary_search(&i);
        if let Ok(idx) = x {
            let end = self.cursor + self.offsets[idx];
            if idx > 0 {
                let start = self.cursor + self.offsets[idx - 1];
                Some(Some(&self.data[start..end]))
            } else {
                Some(Some(&self.data[self.cursor..end]))
            }
        } else {
            if let Ok(_) = self.cols[self.num_not_null_cols..].binary_search(&i) {
                Some(None)
            } else {
                None
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct EncoderRow {
    cols: Vec<u32>,
    null_cols: Vec<u32>,
    offset: Vec<u32>,
    values: Vec<u8>,
    buf: Vec<u8>,
}

impl EncoderRow {
    pub fn append_column(
        &mut self,
        col: u32,
        value: &EncodeValue,
        data_type: &DataType,
    ) -> io::Result<()> {
        if value.is_null() {
            self.null_cols.push(col);
        } else {
            self.cols.push(col);
            value.write_to(&mut self.values, data_type)?;
            self.offset.push(self.values.len() as u32);
        }
        Ok(())
    }

    pub fn to_bytes(&mut self) -> io::Result<&[u8]> {
        self.buf.extend_from_slice(ROW_HEADER);
        self.buf.write_u16::<LittleEndian>(self.cols.len() as u16)?;
        self.buf
            .write_u16::<LittleEndian>(self.null_cols.len() as u16)?;
        for c in self.cols.iter() {
            self.buf.write_u32::<LittleEndian>(*c)?;
        }
        for c in self.null_cols.iter() {
            self.buf.write_u32::<LittleEndian>(*c)?;
        }
        for offset in self.offset.iter() {
            self.buf.write_u32::<LittleEndian>(*offset)?;
        }
        self.buf.extend_from_slice(&self.values);
        Ok(self.buf.as_slice())
    }

    pub fn clear(&mut self) {
        self.offset.clear();
        self.cols.clear();
        self.null_cols.clear();
        self.values.clear();
        self.buf.clear();
    }
}

pub fn encode_value(buf: &mut Vec<u8>, val: &EncodeValue, col: &DataType) -> io::Result<()> {
    val.write_to(buf, col)
}

pub fn get_handle_from_record_key(key: &[u8]) -> &[u8] {
    &key[10..]
}

#[cfg(test)]
mod tests {
    use crate::common::EncodeValue;
    use crate::table::decoder::{DecoderRow, EncoderRow};
    use sqlparser::ast::DataType;

    #[test]
    fn test_decode_and_encode() {
        let mut encoder = EncoderRow::new(Vec::new());
        let col1 = b"abc";
        encoder
            .append_column(0, &EncodeValue::Int(2), &DataType::Int)
            .unwrap();
        encoder
            .append_column(1, &EncodeValue::NULL, &DataType::Int)
            .unwrap();
        encoder
            .append_column(2, &EncodeValue::NULL, &DataType::Int)
            .unwrap();
        encoder
            .append_column(3, &EncodeValue::NULL, &DataType::Int)
            .unwrap();
        encoder
            .append_column(4, &EncodeValue::Bytes(col1.to_vec()), &DataType::String)
            .unwrap();
        encoder
            .append_column(5, &EncodeValue::Int(3), &DataType::Int)
            .unwrap();
        encoder
            .append_column(6, &EncodeValue::NULL, &DataType::Int)
            .unwrap();
        let encode_data = encoder.to_bytes().unwrap();
        let row = DecoderRow::from_bytes(encode_data.to_vec()).unwrap();
        assert_eq!(row.get_data(1), Some(None));
        assert_eq!(row.get_data(2), Some(None));
        assert_eq!(row.get_data(3), Some(None));
        assert_eq!(row.get_data(6), Some(None));
        assert_eq!(row.get_data(7), None);
        let mut col0 = row.get_data(0).unwrap().unwrap();
        let col_val = EncodeValue::read_from(&mut col0, &DataType::Int).unwrap();
        assert_eq!(col_val, EncodeValue::Int(2));
        let mut col4 = row.get_data(4).unwrap().unwrap();
        let col_val = EncodeValue::read_from(&mut col4, &DataType::String).unwrap();
        assert_eq!(col_val, EncodeValue::Bytes(col1.to_vec()));
    }
}
