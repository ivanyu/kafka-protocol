use std::io::{Error, ErrorKind, Read, Result, Write};
use crate::readable_writable::{KafkaReadable, KafkaWritable};
use crate::utils::{read_len_i16, write_len_i16};

impl KafkaReadable for String {
    fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {
        unimplemented!()
    }

    #[inline]
    fn read_ext(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Self> {
        let len = read_len_i16(input, invalid_len_message(field_name), compact)?;
        if len < 0 {
            Err(Error::new(
                ErrorKind::Other,
                format!("non-nullable field {field_name} was serialized as null"),
            ))
        } else {
            read_string(input, len)
        }
    }
}

macro_rules! impl_writable {
    ($type:ty) => {
        impl KafkaWritable for $type {
            fn write(&self, #[allow(unused)] output: &mut impl Write) -> Result<()> {
                unimplemented!()
            }

            #[inline]
            fn write_ext(&self, output: &mut impl Write, field_name: &str, compact: bool) -> Result<()> {
                let len = self.len();
                if len > i16::MAX as usize {
                    Err(Error::new(ErrorKind::Other, invalid_len_message(field_name)(len as i64)))
                } else {
                    write_len_i16(output, invalid_len_message(field_name), len as i16, compact)?;
                    output.write(self.as_bytes()).map(|_| ())
                }
            }
        }
    };
}

impl_writable!(String);
impl_writable!(&str);

impl KafkaReadable for Option<String> {
    fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {
        unimplemented!()
    }

    #[inline]
    fn read_ext(input: &mut impl Read, field_name: &str, compact: bool) -> Result<Self> {
        let len = read_len_i16(input, invalid_len_message(field_name), compact)?;
        if len < 0 {
            Ok(None)
        } else {
            read_string(input, len).map(Some)
        }
    }
}

impl KafkaWritable for Option<String> {
    fn write(&self, #[allow(unused)] output: &mut impl Write) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    fn write_ext(&self, output: &mut impl Write, field_name: &str, compact: bool) -> Result<()> {
        if let Some(string) = self {
            string.write_ext(output, field_name, compact)
        } else {
            write_len_i16(output, invalid_len_message(field_name), -1, compact)
        }
    }
}

#[inline]
fn read_string(input: &mut impl Read, str_len: i16) -> Result<String> {
    let mut buf = vec![0_u8; str_len as usize];
    input.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

#[inline]
fn invalid_len_message(field_name: &str) -> impl FnOnce(i64) -> String {
    let field_name_own = field_name.to_string();
    move |len| {
        format!("string field {field_name_own} had invalid length {len}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use byteorder::{BigEndian, WriteBytesExt};
    use proptest::prelude::*;
    use rstest::rstest;
    use varint_rs::VarintWriter;

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
        original_data.write_ext(&mut cur, "test", compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = Option::<String>::read_ext(&mut cur, "test", compact).unwrap();

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
        original_data.write_ext(&mut cur, "test", compact).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_data = String::read_ext(&mut cur, "test", compact).unwrap();

        assert_eq!(read_data, original_data);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_write_long_string_non_nullable(#[case] compact: bool) {
        let long_string = "a".repeat(i16::MAX as usize + 1);
        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = long_string.write_ext(&mut cur, "test", compact)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_write_long_string_nullable(#[case] compact: bool) {
        let long_string = "a".repeat(i16::MAX as usize + 1);
        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = Some(long_string).write_ext(&mut cur, "test", compact)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }

    #[test]
    fn test_read_null_string_non_nullable_non_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_i16::<BigEndian>(-1).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = String::read_ext(&mut cur, "test", false)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "non-nullable field test was serialized as null");
    }

    #[test]
    fn test_read_null_string_non_nullable_compact() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        cur.write_u32_varint(0).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        let error = String::read_ext(&mut cur, "test", true)
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
        let error = String::read_ext(&mut cur, "test", true)
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
        let error = Option::<String>::read_ext(&mut cur, "test", true)
            .expect_err("must be error");
        assert_eq!(error.to_string(), "string field test had invalid length 32768");
    }
}
