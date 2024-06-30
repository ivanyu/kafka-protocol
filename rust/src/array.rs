use std::io::{Error, ErrorKind, Read, Result, Write};

use varint_rs::{VarintReader, VarintWriter};

use crate::kafka_readable::KafkaReadable;
use crate::kafka_writable::KafkaWritable;
use crate::string::{k_read_string, k_write_string};
use crate::utils::{read_len_i32, write_len_i32};

#[inline]
pub(crate) fn k_read_array<T>(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<T>>
where
    T: KafkaReadable,
{
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null"),
        ))
    } else {
        read_array_inner(input, len)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array<T>(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<Vec<T>>>
where
    T: KafkaReadable,
{
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Ok(None)
    } else {
        read_array_inner(input, len).map(Some)
    }
}

#[inline]
fn read_array_inner<T>(input: &mut impl Read, arr_len: i32) -> Result<Vec<T>>
where
    T: KafkaReadable,
{
    let mut vec: Vec<T> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(T::read(input)?);
    }
    Ok(vec)
}

#[inline]
pub(crate) fn k_read_array_of_strings(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<String>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null"),
        ))
    } else {
        read_array_of_strings_inner(input, field_name, len, compact)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array_of_strings(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<Vec<String>>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Ok(None)
    } else {
        read_array_of_strings_inner(input, field_name, len, compact).map(Some)
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

pub(crate) fn k_write_array<T>(output: &mut impl Write, field_name: &str, array: &[T], compact: bool) -> Result<()>
where
    T: KafkaWritable,
{
    write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
    write_array_inner(output, array)
}

pub(crate) fn k_write_nullable_array<T>(output: &mut impl Write, field_name: &str, array_opt: Option<&[T]>, compact: bool) -> Result<()>
where
    T: KafkaWritable,
{
    if let Some(array) = array_opt {
        write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
        write_array_inner(output, array)
    } else {
        write_len_i32(output, invalid_len_message(field_name), -1, compact)
    }
}

fn write_array_inner<T>(output: &mut impl Write, array: &[T]) -> Result<()>
where
    T: KafkaWritable,
{
    for el in array {
        el.write(output)?
    }
    Ok(())
}

pub(crate) fn k_write_array_of_strings(output: &mut impl Write, field_name: &str, array: &[String], compact: bool) -> Result<()> {
    write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
    write_array_of_strings_inner(output, field_name, array, compact)
}

pub(crate) fn k_write_nullable_array_of_strings(output: &mut impl Write, field_name: &str, array_opt: Option<&[String]>, compact: bool) -> Result<()> {
    if let Some(array) = array_opt {
        write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
        write_array_of_strings_inner(output, field_name, array, compact)
    } else {
        write_len_i32(output, invalid_len_message(field_name), -1, compact)
    }
}

fn write_array_of_strings_inner(output: &mut impl Write, field_name: &str, array: &[String], compact: bool) -> Result<()> {
    for str in array {
        k_write_string(output, field_name, str, compact)?
    }
    Ok(())
}

#[inline]
fn invalid_len_message(field_name: &str) -> impl FnOnce(i64) -> String {
    let field_name_own = field_name.to_string();
    move |len| {
        format!("bytes field {field_name_own} had invalid length {len}")
    }
}
