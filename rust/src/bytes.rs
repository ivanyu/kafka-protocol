use std::io::{Result, Write};

use byteorder::{BigEndian, WriteBytesExt};

pub(crate) fn write_bytes(output: &mut impl Write, value: Option<&[u8]>) -> Result<()> {
    if let Some(v) = value {
        output.write_i32::<BigEndian>(v.len() as i32)?;
        output.write(v).map(|_| ())
    } else {
        output.write_i32::<BigEndian>(-1)
    }
}
