use std::io::{Error, ErrorKind, Read, Result, Write};

use crate::utils::{read_len_i32, write_len_i32};

pub(crate) fn k_read_bytes(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<u8>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null")
        ))
    } else {
        read_bytes(input, len)
    }
}

pub(crate) fn k_read_nullable_bytes(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<Vec<u8>>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Ok(None)
    } else {
        read_bytes(input, len).map(Some)
    }
}

#[inline]
fn read_bytes(input: &mut impl Read, str_len: i32) -> Result<Vec<u8>> {
    let mut buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut buf)?;
    Ok(buf)
}

pub(crate) fn k_write_bytes(output: &mut impl Write, field_name: &str, value: &[u8], compact: bool) -> Result<()> {
    let len = value.len();
    if len > i32::MAX as usize {
        Err(Error::new(ErrorKind::Other, invalid_len_message(field_name)(len as i64)))
    } else {
        write_len_i32(output, invalid_len_message(field_name), len as i32, compact)?;
        output.write(value).map(|_| ())
    }
}

pub(crate) fn k_write_nullable_bytes(output: &mut impl Write, field_name: &str, value: Option<&[u8]>, compact: bool) -> Result<()> {
    if let Some(v) = value {
        k_write_bytes(output, field_name, v, compact)
    } else {
        write_len_i32(output, invalid_len_message(field_name), -1, compact)
    }
}

#[inline]
fn invalid_len_message(field_name: &str) -> impl FnOnce(i64) -> String {
    let field_name_own = field_name.to_string();
    move |len| {
        format!("bytes field {field_name_own} had invalid length {len}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use varint_rs::VarintWriter;
    use proptest::prelude::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(None, false)]
    #[case(None, true)]
    #[case(Some(vec![]), false)]
    #[case(Some(vec![]), true)]
    #[case(Some(vec![1, 2, 3]), false)]
    #[case(Some(vec![1, 2, 3]), true)]
    fn test_serde_nullable(#[case] original_data: Option<Vec<u8>>, #[case] compact: bool) {
        check_serde_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_nullable_non_compact(original_data: Option<Vec<u8>>) {
            check_serde_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_nullable_compact(original_data: Option<Vec<u8>>) {
            check_serde_nullable(original_data, true);
        }
    }

    fn check_serde_nullable(original_data: Option<Vec<u8>>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_nullable_bytes(&mut cur, "test", original_data.as_deref(), compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_nullable_bytes(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case(vec![], false)]
    #[case(vec![], true)]
    #[case(vec![1, 2, 3], false)]
    #[case(vec![1, 2, 3], true)]
    fn test_serde_non_nullable(#[case] original_data: Vec<u8>, #[case] compact: bool) {
        check_serde_non_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_non_nullable_non_compact(original_data: Vec<u8>) {
            check_serde_non_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_non_nullable_compact(original_data: Vec<u8>) {
            check_serde_non_nullable(original_data, true);
        }
    }

    fn check_serde_non_nullable(original_data: Vec<u8>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_bytes(&mut cur, "test", &original_data, compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_bytes(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[test]
    fn test_write_long_bytes_non_nullable() {
        // Unfortunately, creating a Vec of 2147483648 length is too much for unit tests.
    }

    #[test]
    fn test_write_long_bytes_nullable() {
        // Unfortunately, creating a Vec of 2147483648 length is too much for unit tests.
    }

    #[test]
    fn test_read_null_bytes_non_nullable_non_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_i32::<BigEndian>(-1).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_bytes(&mut cur, "test", false)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_null_bytes_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(0).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_bytes(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_long_bytes_non_nullable_non_compact() {
        // There's no point testing this, because we can't write i32 bigger than i32::MAX.
    }

    #[test]
    fn test_read_long_bytes_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i32::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_bytes(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "bytes field test had invalid length 2147483648");
    }

    #[test]
    fn test_read_long_bytes_nullable_non_compact() {
        // There's no point testing this, because we can't write i32 bigger than i32::MAX.
    }

    #[test]
    fn test_read_long_bytes_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i32::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_nullable_bytes(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "bytes field test had invalid length 2147483648");
    }
}
