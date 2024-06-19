use std::io::{Error, ErrorKind, Read, Result};
use crate::kafka_readable::KafkaReadable;
use varint_rs::VarintReader;

pub(crate) fn k_read_bytes(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<u8>> {
    let bytes_len = read_bytes_len(input, compact)?;
    if bytes_len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null")
        ))
    } else {
        read_bytes(input, bytes_len)
    }
}

pub(crate) fn k_read_nullable_bytes(input: &mut impl Read, compact: bool) -> Result<Option<Vec<u8>>> {
    let bytes_len = read_bytes_len(input, compact)?;
    if bytes_len < 0 {
        Ok(None)
    } else {
        read_bytes(input, bytes_len).map(Some)
    }
}

#[inline]
fn read_bytes_len(input: &mut impl Read, compact: bool) -> Result<i32> {
    if compact {
        Ok((input.read_u32_varint()? - 1) as i32)
    } else {
        i32::read(input)
    }
}

#[inline]
fn read_bytes(input: &mut impl Read, str_len: i32) -> Result<Vec<u8>> {
    let mut buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut buf)?;
    Ok(buf)
}
