use std::io::{Error, ErrorKind, Read, Result, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use varint_rs::{VarintReader, VarintWriter};

#[inline]
pub(crate) fn read_len_i16(input: &mut impl Read,
                           invalid_len_message: impl FnOnce(i64) -> String,
                           compact: bool
) -> Result<i16> {
    if compact {
        let len = (input.read_u32_varint()? as i64) - 1;
        if len > i16::MAX as i64 {
            Err(Error::new(ErrorKind::Other, invalid_len_message(len)))
        } else {
            Ok(len as i16)
        }
    } else {
        input.read_i16::<BigEndian>()
    }
}

#[inline]
pub(crate) fn write_len_i16(output: &mut impl Write,
                            invalid_len_message: impl FnOnce(i64) -> String,
                            len: i16,
                            compact: bool
) -> Result<()> {
    if len < -1 {
        Err(Error::new(ErrorKind::Other, invalid_len_message(len as i64)))
    } else {
        if compact {
            output.write_u32_varint((len + 1) as u32)
        } else {
            output.write_i16::<BigEndian>(len)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case(-1, false)]
    #[case(-1, true)]
    #[case(0, false)]
    #[case(0, true)]
    #[case(100, false)]
    #[case(100, true)]
    fn test_i16_success(#[case] len: i16, #[case] compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        write_len_i16(&mut cur, |len| format!("test {len}"), len, compact).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_len = read_len_i16(&mut cur, |len| format!("test {len}"), compact).unwrap();
        assert_eq!(read_len, len);
    }

    #[rstest]
    #[case(-2, false)]
    #[case(-2, true)]
    fn test_i16_invalid(#[case] len: i16, #[case] compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = write_len_i16(&mut cur, |len| format!("test {len}"), len, compact)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "test -2");
    }

    #[test]
    fn test_read_i16_compact_too_big() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i16::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = read_len_i16(&mut cur, |len| format!("test {len}"), true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "test 32768");
    }
}
