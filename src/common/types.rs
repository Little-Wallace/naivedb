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
            EncodeValue::NULL => Ok(()),
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
                output.extend_from_slice(&ENC_EMPTY_PADS[..pad_remain as usize]);
                output.push(ENC_MARKER - ENC_GROUP_SIZE + pad_remain);
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
                output.write_u16::<LE>(*year)?;
                output.write_u8(*month)?;
                output.write_u8(*day)?;
                output.write_u8(*hour)?;
                output.write_u8(*minute)?;
                output.write_u8(*second)?;
                output.write_u32::<LE>(*micro)?;
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
                output.write_u32::<LE>(*micro)?;
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

    pub fn decode_comparable(mut data: &[u8], column_type: &DataType) -> io::Result<EncodeValue> {
        let len = data.len();
        let input = &mut data;
        match column_type {
            DataType::SmallInt => Ok(EncodeValue::Int(input.read_i16::<LE>()?.into())),
            DataType::Int => Ok(EncodeValue::Int(input.read_i32::<LE>()?.into())),
            DataType::BigInt => Ok(EncodeValue::Int(input.read_i64::<LE>()?)),
            DataType::Double => Ok(EncodeValue::Double(input.read_f64::<LE>()?)),
            DataType::Char(_) => {
                let mut output = Vec::with_capacity(data.len());
                decode_comparable_bytes(data, &mut output)?;
                Ok(EncodeValue::Bytes(output))
            }
            DataType::String => {
                let mut output = Vec::with_capacity(data.len());
                decode_comparable_bytes(data, &mut output)?;
                Ok(EncodeValue::Bytes(output))
            }
            DataType::Date => {
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4 {
                    year = input.read_u16::<LE>()?;
                    month = input.read_u8()?;
                    day = input.read_u8()?;
                }
                if len >= 7 {
                    hour = input.read_u8()?;
                    minute = input.read_u8()?;
                    second = input.read_u8()?;
                }
                if len == 11 {
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
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8 {
                    is_negative = input.read_u8()? == 1u8;
                    days = input.read_u32::<LE>()?;
                    hours = input.read_u8()?;
                    minutes = input.read_u8()?;
                    seconds = input.read_u8()?;
                }
                if len == 12 {
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

pub fn decode_comparable_bytes(data: &[u8], output: &mut Vec<u8>) -> io::Result<()> {
    let pad_count = data.len() / (ENC_GROUP_SIZE as usize + 1);
    if data.len() % (ENC_GROUP_SIZE as usize + 1) > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "data can not be decode",
        ));
    }
    for i in 0..pad_count {
        let start = i * (ENC_GROUP_SIZE as usize + 1);
        let end = (i + 1) * (ENC_GROUP_SIZE as usize + 1) - 1;
        if ENC_MARKER - data[end] > 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data can not be decode",
            ));
        }
        let real_size = ENC_GROUP_SIZE - (ENC_MARKER - data[end]);
        if real_size > 0 {
            output.extend_from_slice(&data[start..(start + real_size as usize)]);
        }
    }
    Ok(())
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
