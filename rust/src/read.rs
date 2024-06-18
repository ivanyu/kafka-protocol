use std::io::{Error, ErrorKind, Read, Result};
use byteorder::{BigEndian, ReadBytesExt};
use uuid::Uuid;
use varint_rs::VarintReader;
use crate::types::ReadableStruct;

#[inline]
pub(crate) fn k_read_bool(input: &mut impl Read) -> Result<bool> {
    input.read_i8().map(|v| v != 0)
}

#[inline]
pub(crate) fn k_read_i8(input: &mut impl Read) -> Result<i8> {
    input.read_i8()
}

#[inline]
pub(crate) fn k_read_u16(input: &mut impl Read) -> Result<u16> {
    input.read_u16::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_i16(input: &mut impl Read) -> Result<i16> {
    input.read_i16::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_u32(input: &mut impl Read) -> Result<u32> {
    input.read_u32::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_i32(input: &mut impl Read) -> Result<i32> {
    input.read_i32::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_i64(input: &mut impl Read) -> Result<i64> {
    input.read_i64::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_f64(input: &mut impl Read) -> Result<f64> {
    input.read_f64::<BigEndian>()
}

#[inline]
pub(crate) fn k_read_uuid(input: &mut impl Read) -> Result<Uuid> {
    input.read_u128::<BigEndian>().map(Uuid::from_u128)
}

#[inline]
pub(crate) fn k_read_string(input: &mut impl Read, field_name: &str) -> Result<String> {
    let str_len = read_len(input)?;
    read_string(input, field_name, str_len)
}

#[inline]
pub(crate) fn k_read_compact_string(input: &mut impl Read, field_name: &str) -> Result<String> {
    let str_len = read_compact_len(input, field_name)?;
    read_string(input, field_name, str_len)
}

#[inline]
fn read_string(input: &mut impl Read, field_name: &str, str_len: i16) -> Result<String> {
    check_non_null(field_name, str_len)?;
    read_string_bytes(input, str_len)
}

#[inline]
pub(crate) fn k_read_nullable_string(input: &mut impl Read) -> Result<Option<String>> {
    let str_len = read_len(input)?;
    read_nullable_string(input, str_len)
}

#[inline]
pub(crate) fn k_read_nullable_compact_string(input: &mut impl Read, field_name: &str) -> Result<Option<String>> {
    let str_len = read_compact_len(input, field_name)?;
    read_nullable_string(input, str_len)
}

#[inline]
fn read_nullable_string(input: &mut impl Read, str_len: i16) -> Result<Option<String>> {
    if str_len < 0 {
        Ok(None)
    } else {
        read_string_bytes(input, str_len).map(Some)
    }
}

#[inline]
fn read_len(input: &mut impl Read) -> Result<i16> {
    input.read_i16::<BigEndian>()
}

#[inline]
fn read_compact_len(input: &mut impl Read, field_name: &str) -> Result<i16> {
    let str_len = input.read_u32_varint()? - 1;
    if str_len > 0x7fff {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("string field {} had invalid length ???", field_name)))
    } else {
        Ok(str_len as i16)
    }
}

#[inline]
fn check_non_null(field_name: &str, str_len: i16) -> Result<()> {
    if str_len < 0 {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        Ok(())
    }
}

#[inline]
fn read_string_bytes(input: &mut impl Read, str_len: i16) -> Result<String> {
    let mut str_buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut str_buf)?;
    Ok(String::from_utf8_lossy(&str_buf).to_string())
}

#[inline]
pub(crate) fn k_read_bytes(input: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let str_len = input.read_i32::<BigEndian>()?;
    if str_len < 0 {
        Ok(None)
    } else {
        let mut str_buf = vec![0_u8; str_len as usize];
        input.read_exact(&mut str_buf)?;
        Ok(Some(str_buf))
    }
}

#[inline]
pub(crate) fn k_read_array<T>(input: &mut impl Read, field_name: &str) -> Result<Vec<T>> where T : ReadableStruct
{
    let arr_len = k_read_i32(input)?;
    read_array(input, field_name, arr_len)
}

#[inline]
pub(crate) fn k_read_compact_array<T>(input: &mut impl Read, field_name: &str) -> Result<Vec<T>> where T : ReadableStruct
{
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    read_array(input, field_name, arr_len)
}

#[inline]
pub(crate) fn k_read_nullable_array<T>(input: &mut impl Read) -> Result<Option<Vec<T>>> where T : ReadableStruct
{
    let arr_len = k_read_i32(input)?;
    read_nullable_array(input, arr_len)
}

#[inline]
pub(crate) fn k_read_nullable_compact_array<T>(input: &mut impl Read) -> Result<Option<Vec<T>>> where T : ReadableStruct
{
    let arr_len = (input.read_u32_varint()? as i32) - 1;
    read_nullable_array(input, arr_len)
}

#[inline]
fn read_array<T>(input: &mut impl Read, field_name: &str, arr_len: i32) -> Result<Vec<T>> where T : ReadableStruct
{
    if arr_len < 0 {
        // TODO replace with proper error
        Err(Error::new(ErrorKind::Other, format!("non-nullable field {field_name} was serialized as null")))
    } else {
        read_array_inner(input, arr_len)
    }
}

#[inline]
fn read_nullable_array<T>(input: &mut impl Read, arr_len: i32) -> Result<Option<Vec<T>>> where T : ReadableStruct
{
    if arr_len < 0 {
        Ok(None)
    } else {
        read_array_inner(input, arr_len).map(Some)
    }
}

#[inline]
fn read_array_inner<T>(input: &mut impl Read, arr_len: i32) -> Result<Vec<T>> where T : ReadableStruct {
    let mut vec: Vec<T> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(T::read(input)?);
    }
    Ok(vec)
}

#[inline]
pub(crate) fn k_read_array_of_strings(input: &mut impl Read, field_name: &str) -> Result<Vec<String>> {
    let arr_len = k_read_i32(input)?;
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
    let arr_len = k_read_i32(input)?;
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
        vec.push(k_read_string(input, field_name)?);
    }
    Ok(vec)
}

#[inline]
fn read_array_of_compact_strings_inner(input: &mut impl Read, field_name: &str, arr_len: i32) -> Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(k_read_compact_string(input, field_name)?);
    }
    Ok(vec)
}
