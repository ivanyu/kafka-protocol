use std::io::{Result, Write};

use varint_rs::VarintWriter;

use crate::kafka_writable::KafkaWritable;
use crate::string_write::k_write_string;

pub(crate) fn k_write_array<T>(output: &mut impl Write, array: &[T], compact: bool) -> Result<()> where T : KafkaWritable {
    write_array_len(output, array.len() as i32, compact)?;
    write_array_inner(output, array)
}

pub(crate) fn k_write_nullable_array<T>(output: &mut impl Write, array_opt: Option<&[T]>, compact: bool) -> Result<()> where T : KafkaWritable {
    if let Some(array) = array_opt {
        write_array_len(output, array.len() as i32, compact)?;
        write_array_inner(output, array)
    } else {
        write_array_len(output, -1, compact)
    }
}

fn write_array_inner<T>(output: &mut impl Write, array: &[T]) -> Result<()> where T : KafkaWritable {
    for el in array {
        el.write(output)?
    }
    Ok(())
}

pub(crate) fn k_write_array_of_strings(output: &mut impl Write, array: &[String], compact: bool) -> Result<()>  {
    write_array_len(output, array.len() as i32, compact)?;
    write_array_of_strings_inner(output, array, compact)
}

pub(crate) fn k_write_nullable_array_of_strings(output: &mut impl Write, array_opt: Option<&[String]>, compact: bool) -> Result<()>  {
    if let Some(array) = array_opt {
        write_array_len(output, array.len() as i32, compact)?;
        write_array_of_strings_inner(output, array, compact)
    } else {
        write_array_len(output, -1, compact)
    }
}

fn write_array_of_strings_inner(output: &mut impl Write, array: &[String], compact: bool) -> Result<()> {
    for str in array {
        k_write_string(output, str, compact)?
    }
    Ok(())
}

#[inline]
fn write_array_len(output: &mut impl Write, array_len: i32, compact: bool) -> Result<()> {
    if compact {
        output.write_u32_varint((array_len + 1) as u32)
    } else {
        array_len.write(output)
    }
}
