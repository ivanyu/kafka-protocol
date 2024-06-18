use std::io::{Result, Write};

use byteorder::{BigEndian, WriteBytesExt};
use varint_rs::{VarintWriter};

pub(crate) fn write_string(output: &mut impl Write, value: &str) -> Result<()> {
    write_len(output, value.len() as i16)?;
    write_string_bytes(output, value.as_bytes())
}

pub(crate) fn write_compact_string(output: &mut impl Write, value: &str) -> Result<()> {
    write_compact_len(output, value.len() as i16)?;
    write_string_bytes(output, value.as_bytes())
}

pub(crate) fn write_nullable_string(output: &mut impl Write, value: Option<&str>) -> Result<()> {
    if let Some(v) = value {
        write_string(output, v)
    } else {
        write_len(output, 0)
    }
}

pub(crate) fn write_nullable_compact_string(output: &mut impl Write, value: Option<&str>) -> Result<()> {
    if let Some(v) = value {
        write_compact_string(output, v)
    } else {
        write_compact_len(output, 0)
    }
}

#[inline]
fn write_len(output: &mut impl Write, len: i16) -> Result<()> {
    output.write_i16::<BigEndian>(len)
}

#[inline]
fn write_compact_len(output: &mut impl Write, len: i16) -> Result<()> {
    output.write_u32_varint((len + 1) as u32)
}

#[inline]
fn write_string_bytes(output: &mut impl Write, str: &[u8]) -> Result<()> {
    output.write(str).map(|_| ())
}
