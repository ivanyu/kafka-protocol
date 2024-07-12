use std::io::{Error, ErrorKind, Read, Result, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use varint_rs::{VarintReader, VarintWriter};
use paste::paste;

macro_rules! impl_read_write_len {
    ($type:ty) => {
        paste! {
            #[inline]
            pub(crate) fn [<read_len_ $type>](input: &mut impl Read,
                                       invalid_len_message: impl FnOnce(i64) -> String,
                                       compact: bool
            ) -> Result<$type> {
                if compact {
                    let len = (input.read_u32_varint()? as i64) - 1;
                    if len > $type::MAX as i64 {
                        Err(Error::new(ErrorKind::Other, invalid_len_message(len)))
                    } else {
                        Ok(len as $type)
                    }
                } else {
                    input.[<read_ $type>]::<BigEndian>()
                }
            }

            #[inline]
            pub(crate) fn [<write_len_ $type>](output: &mut impl Write,
                                        invalid_len_message: impl FnOnce(i64) -> String,
                                        len: $type,
                                        compact: bool
            ) -> Result<()> {
                if len < -1 {
                    Err(Error::new(ErrorKind::Other, invalid_len_message(len as i64)))
                } else {
                    if compact {
                        output.write_u32_varint((len + 1) as u32)
                    } else {
                        output.[<write_ $type>]::<BigEndian>(len)
                    }
                }
            }
        }
    }
}

impl_read_write_len!(i16);
impl_read_write_len!(i32);

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};
    use rstest::rstest;
    use super::*;

    macro_rules! test_success {
        ($type:ty) => {
            paste! {
                #[rstest]
                #[case(-1, false)]
                #[case(-1, true)]
                #[case(0, false)]
                #[case(0, true)]
                #[case(100, false)]
                #[case(100, true)]
                fn [<test_ $type _success>](#[case] len: $type, #[case] compact: bool) {
                    let mut cur = Cursor::new(Vec::<u8>::new());
                    [<write_len_ $type>](&mut cur, |len| format!("test {len}"), len, compact).unwrap();
                    cur.seek(SeekFrom::Start(0)).unwrap();
                    let read_len = [<read_len_ $type>](&mut cur, |len| format!("test {len}"), compact).unwrap();
                    assert_eq!(read_len, len);
                }
            }
        }
    }

    test_success!(i16);
    test_success!(i32);

    macro_rules! test_invalid {
        ($type:ty) => {
            paste! {
                #[rstest]
                #[case(-2, false)]
                #[case(-2, true)]
                fn [<test_ $type _invalid>](#[case] len: $type, #[case] compact: bool) {
                    let mut cur = Cursor::new(Vec::<u8>::new());
                    let error = [<write_len_ $type>](&mut cur, |len| format!("test {len}"), len, compact)
                        .expect_err("must be error");
                    assert_eq!(error.to_string(), "test -2");
                }
            }
        }
    }

    test_invalid!(i16);
    test_invalid!(i32);

    macro_rules! test_read_compact_too_big {
        ($type:ty, $error_len:expr) => {
            paste! {
                #[test]
                fn [<test_read $type _compact_too_big>]() {
                    let mut cur = Cursor::new(Vec::<u8>::new());
                    cur.write_u32_varint($type::MAX as u32 + 2).unwrap();
                    cur.seek(SeekFrom::Start(0)).unwrap();
                    let error = [<read_len_ $type>](&mut cur, |len| format!("test {len}"), true)
                        .expect_err("must be error");
                    assert_eq!(error.to_string(), format!("test {}", $error_len));
                }
            }
        }
    }

    test_read_compact_too_big!(i16, "32768");
    test_read_compact_too_big!(i32, "2147483648");
}
