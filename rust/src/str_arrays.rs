use std::io::{Error, ErrorKind, Read, Result, Write};

use crate::strings::{k_read_string, k_write_string};
use crate::utils::{read_len_i32, write_len_i32};

#[inline]
pub(crate) fn k_read_array_of_strings(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<String>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null"),
        ))
    } else {
        read_array_of_strings_inner(input, field_name, len, compact)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array_of_strings(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<Vec<String>>> {
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Ok(None)
    } else {
        read_array_of_strings_inner(input, field_name, len, compact).map(Some)
    }
}

#[inline]
fn read_array_of_strings_inner(input: &mut impl Read, field_name: &str, arr_len: i32, compact: bool) -> Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(k_read_string(input, field_name, compact)?);
    }
    Ok(vec)
}

pub(crate) fn k_write_array_of_strings(output: &mut impl Write, field_name: &str, array: &[String], compact: bool) -> Result<()> {
    write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
    write_array_of_strings_inner(output, field_name, array, compact)
}

pub(crate) fn k_write_nullable_array_of_strings(output: &mut impl Write, field_name: &str, array_opt: Option<&[String]>, compact: bool) -> Result<()> {
    if let Some(array) = array_opt {
        write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
        write_array_of_strings_inner(output, field_name, array, compact)
    } else {
        write_len_i32(output, invalid_len_message(field_name), -1, compact)
    }
}

fn write_array_of_strings_inner(output: &mut impl Write, field_name: &str, array: &[String], compact: bool) -> Result<()> {
    for str in array {
        k_write_string(output, field_name, str, compact)?
    }
    Ok(())
}

#[inline]
fn invalid_len_message(field_name: &str) -> impl FnOnce(i64) -> String {
    let field_name_own = field_name.to_string();
    move |len| {
        format!("string array field {field_name_own} had invalid length {len}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use byteorder::{BigEndian, WriteBytesExt};
    use proptest::proptest;
    use rstest::rstest;
    use varint_rs::VarintWriter;

    use super::*;

    #[rstest]
    #[case(None, false)]
    #[case(None, true)]
    #[case(Some(vec ! []), false)]
    #[case(Some(vec ! []), true)]
    #[case(Some(vec ! ["aaa", "bbb"]), false)]
    #[case(Some(vec ! ["aaa", "bbb"]), true)]
    fn test_serde_nullable(#[case] original_data: Option<Vec<&str>>, #[case] compact: bool) {
        let data: Option<Vec<String>> = original_data.map(
            |v| v.into_iter().map(|e| e.to_owned()
            ).collect());
        check_serde_nullable(data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_nullable_non_compact(original_data: Option<Vec<String>>) {
            check_serde_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_nullable_compact(original_data: Option<Vec<String>>) {
            check_serde_nullable(original_data, true);
        }
    }

    fn check_serde_nullable(original_data: Option<Vec<String>>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_nullable_array_of_strings(&mut cur, "test", original_data.as_deref(), compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_nullable_array_of_strings(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case(vec ! [], false)]
    #[case(vec ! [], true)]
    #[case(vec ! ["aaa", "bbb"], false)]
    #[case(vec ! ["aaa", "bbb"], true)]
    fn test_serde_non_nullable(#[case] original_data: Vec<&str>, #[case] compact: bool) {
        let data: Vec<String> = original_data.into_iter().map(|e| e.to_owned()).collect();
        check_serde_non_nullable(data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_non_nullable_non_compact(original_data: Vec<String>) {
            check_serde_non_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_non_nullable_compact(original_data: Vec<String>) {
            check_serde_non_nullable(original_data, true);
        }
    }

    fn check_serde_non_nullable(original_data: Vec<String>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_array_of_strings(&mut cur, "test", &original_data, compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_array_of_strings(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[test]
    fn test_write_long_array_non_nullable() {
        // Unfortunately, creating a Vec of 2147483648 length is too much for unit tests.
    }

    #[test]
    fn test_write_long_array_nullable() {
        // Unfortunately, creating a Vec of 2147483648 length is too much for unit tests.
    }

    #[test]
    fn test_read_null_array_non_nullable_non_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_i32::<BigEndian>(-1).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_array_of_strings(&mut cur, "test", false)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_null_array_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(0).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_array_of_strings(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_long_array_non_nullable_non_compact() {
        // There's no point testing this, because we can't write i32 bigger than i32::MAX.
    }

    #[test]
    fn test_read_long_array_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i32::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_array_of_strings(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string array field test had invalid length 2147483648");
    }

    #[test]
    fn test_read_long_array_nullable_non_compact() {
        // There's no point testing this, because we can't write i32 bigger than i32::MAX.
    }

    #[test]
    fn test_read_long_array_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(i32::MAX as u32 + 2).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_nullable_array_of_strings(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string array field test had invalid length 2147483648");
    }
}