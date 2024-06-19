use std::io::{Error, ErrorKind, Read, Result};
use varint_rs::VarintReader;
use crate::kafka_readable::KafkaReadable;

pub(crate) fn k_read_string(input: &mut impl Read, field_name: &str, compact: bool) -> Result<String> {
    let str_len = read_str_len(input, field_name, compact)?;
    if str_len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null")
        ))
    } else {
        read_string_bytes(input, str_len)
    }
}

pub(crate) fn k_read_nullable_string(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<String>> {
    let str_len = read_str_len(input, field_name, compact)?;
    if str_len < 0 {
        Ok(None)
    } else {
        read_string_bytes(input, str_len).map(Some)
    }
}

#[inline]
fn read_str_len(input: &mut impl Read, field_name: &str, compact: bool) -> Result<i16> {
    if compact {
        let str_len = input.read_u32_varint()? - 1;
        if str_len > 0x7fff {
            Err(Error::new(
                ErrorKind::Other,
                format!("string field {field_name} had invalid length {str_len}")
            ))
        } else {
            Ok(str_len as i16)
        }
    } else {
        i16::read(input)
    }
}

#[inline]
fn read_string_bytes(input: &mut impl Read, str_len: i16) -> Result<String> {
    let mut str_buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut str_buf)?;
    Ok(String::from_utf8_lossy(&str_buf).to_string())
}
