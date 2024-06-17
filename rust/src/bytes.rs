use std::io::{Read, Result, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub(crate) fn read_bytes(input: &mut impl Read, #[allow(unused)] field_name: &str) -> Result<Option<Vec<u8>>> {
    let str_len = input.read_i32::<BigEndian>()?;
    if str_len < 0 {
        Ok(None)
    } else {
        let mut str_buf = vec![0_u8; str_len as usize];
        input.read_exact(&mut str_buf)?;
        Ok(Some(str_buf))
    }
}

pub(crate) fn write_bytes(output: &mut impl Write, value: Option<&[u8]>) -> Result<()> {
    if let Some(v) = value {
        output.write_i32::<BigEndian>(v.len() as i32)?;
        output.write(v).map(|_| ())
    } else {
        output.write_i32::<BigEndian>(-1)
    }
}
