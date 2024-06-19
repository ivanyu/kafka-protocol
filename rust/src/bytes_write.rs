use std::io::{Result, Write};

use byteorder::{BigEndian, WriteBytesExt};
use varint_rs::VarintWriter;

pub(crate) fn k_write_bytes(output: &mut impl Write, value: &[u8], compact: bool) -> Result<()> {
    if compact {
        write_compact_len(output, value.len() as i16)?;
    } else {
        write_len(output, value.len() as i16)?;
    }
    output.write(value).map(|_| ())
}

pub(crate) fn k_write_nullable_bytes(output: &mut impl Write, value: Option<&[u8]>, compact: bool) -> Result<()> {
    if let Some(v) = value {
        k_write_bytes(output, v, compact)
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
