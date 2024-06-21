use std::io::{Error, ErrorKind, Read, Result};
use varint_rs::VarintReader;
use crate::kafka_readable::KafkaReadable;
use crate::string::k_read_string;

#[inline]
pub(crate) fn k_read_array<T>(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<T>> where T : KafkaReadable {
    let arr_len = read_array_len(input, compact)?;
    if arr_len < 0 {
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_inner(input, arr_len)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array<T>(input: &mut impl Read, compact: bool) -> Result<Option<Vec<T>>> where T : KafkaReadable {
    let arr_len = read_array_len(input, compact)?;
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
pub(crate) fn k_read_array_of_strings(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<String>> {
    let arr_len = read_array_len(input, compact)?;
    if arr_len < 0 {
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_of_strings_inner(input, field_name, arr_len, compact)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array_of_strings(input: &mut impl Read, compact: bool) -> Result<Option<Vec<String>>> {
    let arr_len = read_array_len(input, compact)?;
    if arr_len < 0 {
        Ok(None)
    } else {
        read_array_of_strings_inner(input, "", arr_len, compact).map(Some)
    }
}

#[inline]
fn read_array_of_strings_inner(input: &mut impl Read, field_name: &str, arr_len: i32, compact: bool) -> Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(k_read_string(input, field_name, compact)?);
    }
    Ok(vec)
}

#[inline]
fn read_array_len(input: &mut impl Read, compact: bool) -> Result<i32> {
    if compact {
        Ok((input.read_u32_varint()? as i32) - 1)
    } else {
        i32::read(input)
    }
}
