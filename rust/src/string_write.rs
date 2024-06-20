use std::io::{Result, Write};

use byteorder::{BigEndian, WriteBytesExt};
use varint_rs::VarintWriter;

pub(crate) fn k_write_string(output: &mut impl Write, string: &str, compact: bool) -> Result<()> {
    if compact {
        write_compact_len(output, string.len() as i16)?;
    } else {
        write_len(output, string.len() as i16)?;
    }
    output.write(string.as_bytes()).map(|_| ())
}

pub(crate) fn k_write_nullable_string(output: &mut impl Write, string_opt: Option<&str>, compact: bool) -> Result<()> {
    if let Some(string) = string_opt {
        k_write_string(output, string, compact)
    } else {
        if compact {
            write_compact_len(output, -1)
        } else {
            write_len(output, -1)
        }
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
