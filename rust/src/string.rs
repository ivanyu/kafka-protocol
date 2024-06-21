use std::io::{Error, ErrorKind, Read, Result, Write};
use byteorder::{BigEndian, WriteBytesExt};
use varint_rs::{VarintReader, VarintWriter};
use crate::kafka_readable::KafkaReadable;

const MAX_STRING_LEN: i16 = i16::MAX;  // 0x7fff

pub(crate) fn k_read_string(input: &mut impl Read, field_name: &str, compact: bool) -> Result<String> {
    let str_len = read_str_len(input, field_name, compact)?;
    if str_len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null")
        ))
    } else {
        read_string_bytes(input, str_len)
    }
}

pub(crate) fn k_read_nullable_string(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<String>> {
    let str_len = read_str_len(input, field_name, compact)?;
    if str_len < 0 {
        Ok(None)
    } else {
        read_string_bytes(input, str_len).map(Some)
    }
}

#[inline]
fn read_str_len(input: &mut impl Read, field_name: &str, compact: bool) -> Result<i16> {
    if compact {
        let str_len = (input.read_u32_varint()? as i64) - 1;
        if str_len > MAX_STRING_LEN as i64 {
            Err(Error::new(
                ErrorKind::Other,
                format!("string field {field_name} had invalid length {str_len}")
            ))
        } else {
            Ok(str_len as i16)
        }
    } else {
        i16::read(input)
    }
}

#[inline]
fn read_string_bytes(input: &mut impl Read, str_len: i16) -> Result<String> {
    let mut str_buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut str_buf)?;
    Ok(String::from_utf8_lossy(&str_buf).to_string())
}

pub(crate) fn k_write_string(output: &mut impl Write, field_name: &str, string: &str, compact: bool) -> Result<()> {
    let str_len = string.len();
    if str_len > MAX_STRING_LEN as usize {
        return Err(Error::new(
            ErrorKind::Other,
            format!("string field {field_name} had invalid length {str_len}")
        ));
    }

    write_str_len(output, str_len as i16, compact)?;
    output.write(string.as_bytes()).map(|_| ())
}

pub(crate) fn k_write_nullable_string(output: &mut impl Write, field_name: &str, string_opt: Option<&str>, compact: bool) -> Result<()> {
    if let Some(string) = string_opt {
        k_write_string(output, field_name, string, compact)
    } else {
        write_str_len(output, -1, compact)
    }
}

#[inline]
fn write_str_len(output: &mut impl Write, len: i16, compact: bool) -> Result<()> {
    if compact {
        output.write_u32_varint((len + 1) as u32)
    } else {
        output.write_i16::<BigEndian>(len)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};
    use proptest::prelude::*;
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case(None, false)]
    #[case(None, true)]
    #[case(Some("".to_string()), false)]
    #[case(Some("".to_string()), true)]
    #[case(Some("aaa".to_string()), false)]
    #[case(Some("aaa".to_string()), true)]
    fn test_serde_nullable(#[case] original_data: Option<String>, #[case] compact: bool) {
        check_serde_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_nullable_non_compact(original_data: Option<String>) {
            check_serde_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_nullable_compact(original_data: Option<String>) {
            check_serde_nullable(original_data, true);
        }
    }

    fn check_serde_nullable(original_data: Option<String>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_nullable_string(&mut cur, "test", original_data.as_deref(), compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_nullable_string(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case("".to_string(), false)]
    #[case("".to_string(), true)]
    #[case("aaa".to_string(), false)]
    #[case("aaa".to_string(), true)]
    fn test_serde_non_nullable(#[case] original_data: String, #[case] compact: bool) {
        check_serde_non_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_non_nullable_non_compact(original_data: String) {
            check_serde_non_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_non_nullable_compact(original_data: String) {
            check_serde_non_nullable(original_data, true);
        }
    }

    fn check_serde_non_nullable(original_data: String, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_string(&mut cur, "test", &original_data, compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_string(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_write_long_string_non_nullable(#[case] compact: bool) {
        let long_string = "a".repeat(i16::MAX as usize + 1);
        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = k_write_string(&mut cur, "test", &long_string, compact)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_write_long_string_nullable(#[case] compact: bool) {
        let long_string = "a".repeat(i16::MAX as usize + 1);
        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = k_write_nullable_string(&mut cur, "test", Some(&long_string), compact)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }

    #[test]
    fn test_read_null_string_non_nullable_non_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_i16::<BigEndian>(-1).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_string(&mut cur, "test", false)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_null_string_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(0).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_string(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_long_string_non_nullable_non_compact() {
        // There's no point testing this, because we can't write i16 bigger than i16::MAX.
    }

    #[test]
    fn test_read_long_string_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i16::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_string(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }

    #[test]
    fn test_read_long_string_nullable_non_compact() {
        // There's no point testing this, because we can't write i16 bigger than i16::MAX.
    }

    #[test]
    fn test_read_long_string_ullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i16::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_nullable_string(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }
}
