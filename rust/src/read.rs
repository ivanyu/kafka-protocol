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

#[inline]
pub(crate) fn k_read_array<T>(input: &mut impl Read, field_name: &str) -> Result<Vec<T>> where T : KafkaReadable {
    let arr_len = i32::read(input)?;
    read_array(input, field_name, arr_len)
}

#[inline]
pub(crate) fn k_read_compact_array<T>(input: &mut impl Read, field_name: &str) -> Result<Vec<T>> where T : KafkaReadable {
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    read_array(input, field_name, arr_len)
}

#[inline]
pub(crate) fn k_read_nullable_array<T>(input: &mut impl Read) -> Result<Option<Vec<T>>> where T : KafkaReadable {
    let arr_len = i32::read(input)?;
    read_nullable_array(input, arr_len)
}

#[inline]
pub(crate) fn k_read_nullable_compact_array<T>(input: &mut impl Read) -> Result<Option<Vec<T>>> where T : KafkaReadable {
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    read_nullable_array(input, arr_len)
}

#[inline]
fn read_array<T>(input: &mut impl Read, field_name: &str, arr_len: i32) -> Result<Vec<T>> where T : KafkaReadable {
    if arr_len < 0 {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_inner(input, arr_len)
    }
}

#[inline]
fn read_nullable_array<T>(input: &mut impl Read, arr_len: i32) -> Result<Option<Vec<T>>> where T : KafkaReadable {
    if arr_len < 0 {
        Ok(None)
    } else {
        read_array_inner(input, arr_len).map(Some)
    }
}

#[inline]
fn read_array_inner<T>(input: &mut impl Read, arr_len: i32) -> Result<Vec<T>> where T : KafkaReadable {
    let mut vec: Vec<T> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(T::read(input)?);
    }
    Ok(vec)
}

#[inline]
pub(crate) fn k_read_array_of_strings(input: &mut impl Read, field_name: &str) -> Result<Vec<String>> {
    let arr_len = i32::read(input)?;
    if arr_len < 0 {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_of_strings_inner(input, field_name, arr_len)
    }
}

#[inline]
pub(crate) fn k_read_compact_array_of_strings(input: &mut impl Read, field_name: &str) -> Result<Vec<String>> {
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    if arr_len < 0 {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_of_compact_strings_inner(input, field_name, arr_len)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array_of_strings(input: &mut impl Read) -> Result<Option<Vec<String>>> {
    let arr_len = i32::read(input)?;
    if arr_len < 0 {
        Ok(None)
    } else {
        read_array_of_strings_inner(input, "", arr_len).map(Some)
    }
}

#[inline]
pub(crate) fn k_read_nullable_compact_array_of_strings(input: &mut impl Read) -> Result<Option<Vec<String>>> {
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    if arr_len < 0 {
        Ok(None)
    } else {
        read_array_of_compact_strings_inner(input, "", arr_len).map(Some)
    }
}

#[inline]
fn read_array_of_strings_inner(input: &mut impl Read, field_name: &str, arr_len: i32) -> Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(k_read_string(input, field_name, false)?);
    }
    Ok(vec)
}

#[inline]
fn read_array_of_compact_strings_inner(input: &mut impl Read, field_name: &str, arr_len: i32) -> Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(k_read_string(input, field_name, true)?);
    }
    Ok(vec)
}
