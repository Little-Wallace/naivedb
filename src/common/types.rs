use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use sqlparser::ast::DataType;
use sqlparser::ast::Value;
use std::io;
use std::io::Read;

const ENC_GROUP_SIZE: u8 = 8;
const ENC_EMPTY_PADS: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
const ENC_MARKER: u8 = 255;

#[derive(Clone, Debug)]
pub enum EncodeValue {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

// TODO: Support compare float and double and date and time.
impl std::cmp::PartialEq for EncodeValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            EncodeValue::NULL => *other == EncodeValue::NULL,
            EncodeValue::Int(v) => {
                if let EncodeValue::Int(o) = other {
                    *v == *o
                } else {
                    false
                }
            }
            EncodeValue::Bytes(v) => {
                if let EncodeValue::Bytes(o) = other {
                    v.eq(o)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl EncodeValue {
    pub fn from_parse_value(val: Value) -> io::Result<EncodeValue> {
        match val {
            Value::Boolean(v) => {
                if v {
                    Ok(EncodeValue::Int(1))
                } else {
                    Ok(EncodeValue::Int(0))
                }
            }
            Value::DoubleQuotedString(v) => Ok(EncodeValue::Bytes(v.into_bytes())),
            Value::SingleQuotedString(v) => Ok(EncodeValue::Bytes(v.into_bytes())),
            Value::HexStringLiteral(v) => Ok(EncodeValue::Bytes(v.into_bytes())),
            Value::Null => Ok(EncodeValue::NULL),
            Value::NationalStringLiteral(v) => Ok(EncodeValue::Bytes(v.into_bytes())),
            #[cfg(not(feature = "bigdecimal"))]
            Value::Number(v, _) => {
                Ok(EncodeValue::Int(v.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "parse error")
                })?))
            }
            #[cfg(feature = "bigdecimal")]
            Value::Number(v, _) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not support decimal",
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not support decimal",
            )),
        }
    }

    pub fn null() -> EncodeValue {
        EncodeValue::NULL
    }

    pub fn is_null(&self) -> bool {
        match self {
            EncodeValue::NULL => true,
            _ => false,
        }
    }

    pub fn encode_comparable(
        &self,
        output: &mut Vec<u8>,
        column_type: &DataType,
    ) -> io::Result<()> {
        match self {
            EncodeValue::NULL => {
                output.extend_from_slice(ENC_EMPTY_PADS);
                output.push(ENC_MARKER - ENC_GROUP_SIZE);
                Ok(())
            }
            EncodeValue::Bytes(v) => {
                let pad_count = v.len() / ENC_GROUP_SIZE as usize;
                let pad_remain = (v.len() % ENC_GROUP_SIZE as usize) as u8;
                for i in 0..pad_count {
                    output.extend_from_slice(
                        &v[i * ENC_GROUP_SIZE as usize..(i + 1) * ENC_GROUP_SIZE as usize],
                    );
                    output.push(ENC_MARKER);
                }
                if pad_remain > 0 {
                    output.extend_from_slice(&v[pad_count * ENC_GROUP_SIZE as usize..]);
                }
                output.extend_from_slice(&ENC_EMPTY_PADS[pad_remain as usize..]);
                output.push(ENC_MARKER - ENC_GROUP_SIZE + pad_remain);
                Ok(())
            }
            EncodeValue::Float(v) => {
                output.write_f32::<LE>(*v)?;
                output.extend_from_slice(&ENC_EMPTY_PADS[4..]);
                output.push(ENC_MARKER - ENC_GROUP_SIZE + 4);
                Ok(())
            }
            EncodeValue::Double(v) => {
                output.write_f64::<LE>(*v)?;
                output.extend_from_slice(ENC_EMPTY_PADS);
                output.push(ENC_MARKER - ENC_GROUP_SIZE);
                Ok(())
            }
            EncodeValue::Int(v) => {
                match column_type {
                    DataType::SmallInt | DataType::Int | DataType::BigInt => {
                        output.write_i64::<LE>(*v)?
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid column type",
                        ))
                    }
                }
                output.extend_from_slice(ENC_EMPTY_PADS);
                output.push(ENC_MARKER - ENC_GROUP_SIZE);
                Ok(())
            }
            EncodeValue::Date(year, month, day, hour, minute, second, micro) => {
                output.write_u32::<LE>(*year as u32)?;
                output.write_u8(*month)?;
                output.write_u8(*day)?;
                output.write_u8(*hour)?;
                output.write_u8(*minute)?;
                output.push(ENC_MARKER);
                output.write_u8(*second)?;
                output.write_u32::<LE>(*micro)?;
                output.extend_from_slice(&ENC_EMPTY_PADS[5..]);
                output.push(ENC_MARKER - ENC_GROUP_SIZE + 5);
                Ok(())
            }
            EncodeValue::Time(negative, day, hour, minute, second, micro) => {
                if *negative {
                    output.write_u8(1)?;
                } else {
                    output.write_u8(0)?;
                }
                output.write_u32::<LE>(*day)?;
                output.write_u8(*hour)?;
                output.write_u8(*minute)?;
                output.write_u8(*second)?;
                output.push(ENC_MARKER);
                output.write_u32::<LE>(*micro)?;
                output.extend_from_slice(&ENC_EMPTY_PADS[4..]);
                output.push(ENC_MARKER - ENC_GROUP_SIZE + 4);
                Ok(())
            }
        }
    }

    pub fn encode(&self, output: &mut Vec<u8>, column_type: &DataType) -> io::Result<()> {
        match self {
            EncodeValue::NULL => Ok(()),
            EncodeValue::Bytes(v) => {
                output.write_u32::<LE>(v.len() as u32)?;
                output.extend_from_slice(v);
                Ok(())
            }
            EncodeValue::Float(v) => output.write_f32::<LE>(*v),
            EncodeValue::Double(v) => output.write_f64::<LE>(*v),
            EncodeValue::Int(v) => match column_type {
                DataType::SmallInt => output.write_i16::<LE>(*v as i16),
                DataType::Int => output.write_i32::<LE>(*v as i32),
                DataType::BigInt => output.write_i64::<LE>(*v as i64),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid column type",
                )),
            },
            EncodeValue::Date(year, month, day, hour, minute, second, micro) => {
                if *micro > 0 {
                    output.write_u8(11)?;
                } else {
                    output.write_u8(7)?;
                }
                output.write_u16::<LE>(*year)?;
                output.write_u8(*month)?;
                output.write_u8(*day)?;
                output.write_u8(*hour)?;
                output.write_u8(*minute)?;
                output.write_u8(*second)?;
                if *micro > 0 {
                    output.write_u32::<LE>(*micro)?;
                }
                Ok(())
            }
            EncodeValue::Time(negative, day, hour, minute, second, micro) => {
                output.write_u8(12)?;
                if *negative {
                    output.write_u8(1)?;
                } else {
                    output.write_u8(0)?;
                }
                output.write_u32::<LE>(*day)?;
                output.write_u8(*hour)?;
                output.write_u8(*minute)?;
                output.write_u8(*second)?;
                output.write_u32::<LE>(*micro)?;
                Ok(())
            }
        }
    }

    pub fn decode_comparable(input: &mut &[u8], column_type: &DataType) -> io::Result<EncodeValue> {
        match column_type {
            DataType::SmallInt | DataType::Int | DataType::BigInt => {
                let value = EncodeValue::Int(input.read_i64::<LE>()?);
                check_and_skip_empty_padding(input)?;
                Ok(value)
            }
            DataType::Double => {
                let value = EncodeValue::Double(input.read_f64::<LE>()?);
                check_and_skip_empty_padding(input)?;
                Ok(value)
            }
            DataType::Char(_) | DataType::String => {
                let mut output = Vec::new();
                decode_comparable_bytes(input, &mut output)?;
                Ok(EncodeValue::Bytes(output))
            }
            DataType::Date => {
                let year = input.read_u32::<LE>()? as u16;
                let month = input.read_u8()?;
                let day = input.read_u8()?;
                let hour = input.read_u8()?;
                let minute = input.read_u8()?;
                input.read_u8()?;
                let second = input.read_u8()?;
                let micro_second = input.read_u32::<LE>()?;
                input.read_u24::<LE>()?;
                if input.read_u8()? != ENC_MARKER - ENC_GROUP_SIZE + 5 {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "data decode corruption",
                    ))
                } else {
                    Ok(EncodeValue::Date(
                        year,
                        month,
                        day,
                        hour,
                        minute,
                        second,
                        micro_second,
                    ))
                }
            }
            DataType::Time => {
                let is_negative = input.read_u8()? == 1u8;
                let days = input.read_u32::<LE>()?;
                let hours = input.read_u8()?;
                let minutes = input.read_u8()?;
                let seconds = input.read_u8()?;
                input.read_u8()?;
                let micro_seconds = input.read_u32::<LE>()?;
                input.read_u32::<LE>()?;
                if input.read_u8()? != ENC_MARKER - ENC_GROUP_SIZE + 4 {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "data decode corruption",
                    ))
                } else {
                    Ok(EncodeValue::Time(
                        is_negative,
                        days,
                        hours,
                        minutes,
                        seconds,
                        micro_seconds,
                    ))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid column type",
            )),
        }
    }

    pub fn read_from(input: &mut &[u8], column_type: &DataType) -> io::Result<EncodeValue> {
        match column_type {
            DataType::SmallInt => Ok(EncodeValue::Int(input.read_i16::<LE>()?.into())),
            DataType::Int => Ok(EncodeValue::Int(input.read_i32::<LE>()?.into())),
            DataType::BigInt => Ok(EncodeValue::Int(input.read_i64::<LE>()?)),
            DataType::Double => Ok(EncodeValue::Double(input.read_f64::<LE>()?)),
            DataType::Char(_) => {
                let l = input.read_u32::<LE>()?;
                let mut v = vec![0; l as usize];
                input.read_exact(&mut v)?;
                Ok(EncodeValue::Bytes(v))
            }
            DataType::String => {
                let l = input.read_u32::<LE>()?;
                let mut v = vec![0; l as usize];
                input.read_exact(&mut v)?;
                Ok(EncodeValue::Bytes(v))
            }
            DataType::Date => {
                let len = input.read_u8()?;
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = input.read_u16::<LE>()?;
                    month = input.read_u8()?;
                    day = input.read_u8()?;
                }
                if len >= 7u8 {
                    hour = input.read_u8()?;
                    minute = input.read_u8()?;
                    second = input.read_u8()?;
                }
                if len == 11u8 {
                    micro_second = input.read_u32::<LE>()?;
                }
                Ok(EncodeValue::Date(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    micro_second,
                ))
            }
            DataType::Time => {
                let len = input.read_u8()?;
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = input.read_u8()? == 1u8;
                    days = input.read_u32::<LE>()?;
                    hours = input.read_u8()?;
                    minutes = input.read_u8()?;
                    seconds = input.read_u8()?;
                }
                if len == 12u8 {
                    micro_seconds = input.read_u32::<LE>()?;
                }
                Ok(EncodeValue::Time(
                    is_negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    micro_seconds,
                ))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid column type",
            )),
        }
    }
}

pub fn check_and_skip_empty_padding(input: &mut &[u8]) -> io::Result<()> {
    input.read_i64::<LE>()?;
    if input.read_u8()? != ENC_MARKER - ENC_GROUP_SIZE {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "data decode corruption",
        ))
    } else {
        Ok(())
    }
}

pub fn decode_comparable_bytes(input: &mut &[u8], output: &mut Vec<u8>) -> io::Result<()> {
    let mut buf = vec![0u8; 9];
    while input.len() > ENC_GROUP_SIZE as usize {
        input.read_exact(&mut buf)?;
        if buf[ENC_GROUP_SIZE as usize] < ENC_MARKER {
            let empty_len = ENC_MARKER - buf[8];
            if empty_len < ENC_GROUP_SIZE {
                output.extend_from_slice(&buf[..(ENC_GROUP_SIZE - empty_len) as usize]);
            }
            return Ok(());
        } else {
            output.extend_from_slice(&buf);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "data decode corruption",
    ))
}

impl From<EncodeValue> for String {
    fn from(v: EncodeValue) -> Self {
        match v {
            EncodeValue::NULL => "NULL".to_string(),
            EncodeValue::Bytes(v) => String::from_utf8(v).unwrap(),
            EncodeValue::Int(v) => v.to_string(),
            EncodeValue::Float(v) => v.to_string(),
            EncodeValue::Double(v) => v.to_string(),
            EncodeValue::Date(year, month, day, hour, min, sec, micro) => format!(
                "{}-{}-{} {}:{}:{}.{}",
                year, month, day, hour, min, sec, micro
            ),
            EncodeValue::Time(_, day, hour, min, sec, micro) => {
                format!("{}d {}:{}:{}.{}", day, hour, min, sec, micro)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_and_encode() {
        let value1 = "abc".to_string();
        let ecv1 = EncodeValue::from_parse_value(Value::SingleQuotedString(value1)).unwrap();
        let ecv2 = EncodeValue::Int(15);
        let mut data = vec![];
        ecv1.encode_comparable(&mut data, &DataType::String)
            .unwrap();
        ecv2.encode_comparable(&mut data, &DataType::Int).unwrap();

        let mut input = data.as_slice();
        let dcv1 = EncodeValue::decode_comparable(&mut input, &DataType::String).unwrap();
        let dcv2 = EncodeValue::decode_comparable(&mut input, &DataType::Int).unwrap();
        assert_eq!(ecv1, dcv1);
        assert_eq!(ecv2, dcv2);
    }

    #[test]
    fn test_comparable_prefix() {
        let key1 = "".to_string();
        let key2 = "bcdefghi".to_string();
        let key3 = "abcd".to_string();
        let key4 = "efgh".to_string();
        let ecv1 = EncodeValue::from_parse_value(Value::SingleQuotedString(key1)).unwrap();
        let ecv2 = EncodeValue::from_parse_value(Value::SingleQuotedString(key2)).unwrap();
        let ecv3 = EncodeValue::from_parse_value(Value::SingleQuotedString(key3)).unwrap();
        let ecv4 = EncodeValue::from_parse_value(Value::SingleQuotedString(key4)).unwrap();

        let mut data1 = vec![];
        ecv1.encode_comparable(&mut data1, &DataType::String)
            .unwrap();
        ecv2.encode_comparable(&mut data1, &DataType::String)
            .unwrap();

        let mut data2 = vec![];
        ecv3.encode_comparable(&mut data2, &DataType::String)
            .unwrap();
        ecv4.encode_comparable(&mut data2, &DataType::String)
            .unwrap();

        assert_eq!(std::cmp::Ordering::Less, data1.cmp(&data2));
    }
}
