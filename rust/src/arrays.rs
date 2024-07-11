use std::io::{Error, ErrorKind, Read, Result, Write};

use crate::readable_writable::{KafkaReadable, KafkaWritable};
use crate::utils::{read_len_i32, write_len_i32};

#[inline]
pub(crate) fn k_read_array<T>(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Vec<T>>
where
    T: KafkaReadable,
{
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Err(Error::new(
            ErrorKind::Other,
            format!("non-nullable field {field_name} was serialized as null"),
        ))
    } else {
        read_array_inner(input, len)
    }
}

#[inline]
pub(crate) fn k_read_nullable_array<T>(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Option<Vec<T>>>
where
    T: KafkaReadable,
{
    let len = read_len_i32(input, invalid_len_message(field_name), compact)?;
    if len < 0 {
        Ok(None)
    } else {
        read_array_inner(input, len).map(Some)
    }
}

#[inline]
fn read_array_inner<T>(input: &mut impl Read, arr_len: i32) -> Result<Vec<T>>
where
    T: KafkaReadable,
{
    let mut vec: Vec<T> = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        vec.push(T::read(input)?);
    }
    Ok(vec)
}

pub(crate) fn k_write_array<T>(output: &mut impl Write, field_name: &str, array: &[T], compact: bool) -> Result<()>
where
    T: KafkaWritable,
{
    write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
    write_array_inner(output, array)
}

pub(crate) fn k_write_nullable_array<T>(output: &mut impl Write, field_name: &str, array_opt: Option<&[T]>, compact: bool) -> Result<()>
where
    T: KafkaWritable,
{
    if let Some(array) = array_opt {
        write_len_i32(output, invalid_len_message(field_name), array.len() as i32, compact)?;
        write_array_inner(output, array)
    } else {
        write_len_i32(output, invalid_len_message(field_name), -1, compact)
    }
}

fn write_array_inner<T>(output: &mut impl Write, array: &[T]) -> Result<()>
where
    T: KafkaWritable,
{
    for el in array {
        el.write(output)?
    }
    Ok(())
}

#[inline]
fn invalid_len_message(field_name: &str) -> impl FnOnce(i64) -> String {
    let field_name_own = field_name.to_string();
    move |len| {
        format!("array field {field_name_own} had invalid length {len}")
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
    #[case(Some(vec![]), false)]
    #[case(Some(vec![]), true)]
    #[case(Some(vec![1, 2, 3]), false)]
    #[case(Some(vec![1, 2, 3]), true)]
    fn test_serde_nullable(#[case] original_data: Option<Vec<i32>>, #[case] compact: bool) {
        check_serde_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_nullable_non_compact(original_data: Option<Vec<i32>>) {
            check_serde_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_nullable_compact(original_data: Option<Vec<i32>>) {
            check_serde_nullable(original_data, true);
        }
    }

    fn check_serde_nullable(original_data: Option<Vec<i32>>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_nullable_array(&mut cur, "test", original_data.as_deref(), compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_nullable_array::<i32>(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case(vec![], false)]
    #[case(vec![], true)]
    #[case(vec![1, 2, 3], false)]
    #[case(vec![1, 2, 3], true)]
    fn test_serde_non_nullable(#[case] original_data: Vec<i32>, #[case] compact: bool) {
        check_serde_non_nullable(original_data, compact);
    }

    proptest! {
        #[test]
        fn test_prop_serde_non_nullable_non_compact(original_data: Vec<i32>) {
            check_serde_non_nullable(original_data, false);
        }

        #[test]
        fn test_prop_serde_non_nullable_compact(original_data: Vec<i32>) {
            check_serde_non_nullable(original_data, true);
        }
    }

    fn check_serde_non_nullable(original_data: Vec<i32>, compact: bool) {
        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_array(&mut cur, "test", &original_data, compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = k_read_array::<i32>(&mut cur, "test", compact).unwrap();

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
        let error = k_read_array::<i32>(&mut cur, "test", false)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_null_array_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(0).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = k_read_array::<i32>(&mut cur, "test", true)
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
        let error = k_read_array::<i32>(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "array field test had invalid length 2147483648");
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
        let error = k_read_nullable_array::<i32>(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "array field test had invalid length 2147483648");
    }
}
