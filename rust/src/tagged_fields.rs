use std::io::{Error, ErrorKind, Read, Result, Write};
use serde::{Deserialize, Serialize};
use crate::primitives::{KafkaReadable, KafkaWritable};
#[cfg(test)] use proptest_derive::Arbitrary;
use crate::arrays::{k_read_array, k_write_array};
#[cfg(test)] use crate::test_utils::proptest_strategies;
#[cfg(test)] use crate::test_utils::serde_bytes;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct RawTaggedField {
    pub tag: i32,
    #[cfg_attr(test, proptest(strategy = "proptest_strategies::vec()"))]
    #[cfg_attr(test, serde(with="serde_bytes"))]
    pub data: Vec<u8>,
}

impl KafkaReadable for RawTaggedField {
    fn read(input: &mut impl Read) -> Result<Self> {
        let tag = i32::read(input)?;
        let data = k_read_array::<u8>(input, "topics", true)?;
        Ok(RawTaggedField { tag, data })
    }
}

impl KafkaWritable for RawTaggedField {
    fn write(&self, output: &mut impl Write) -> Result<()> {
        self.tag.write(output)?;
        k_write_array(output, "data", &self.data, true)?;
        Ok(())
    }
}

pub(crate) fn k_read_unknown_tagged_fields(input: &mut impl Read, field_name: &str) -> Result<Vec<RawTaggedField>> {
    k_read_array::<RawTaggedField>(input, field_name, true)
}

pub(crate) fn k_write_unknown_tagged_fields(output: &mut impl Write, field_name: &str, fields: &[RawTaggedField]) -> Result<()> {
    for x in fields.windows(2) {
        let tag0 = &x[0].tag;
        let tag1 = &x[1].tag;
        if tag0 >= tag1 {
            return Err(Error::new(ErrorKind::Other, format!(
                "Invalid raw tag field list: tag {tag1:?} comes after tag {tag0:?}, but is not higher than it."
            )));
        }
    }

    k_write_array(output, field_name, fields, true)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_serde(data: RawTaggedField) {
            crate::test_utils::test_serde(data)?;
        }
    }

    #[test]
    fn test_serde_multiple_fields() {
        let original_fields = vec! {
            RawTaggedField { tag: 0, data: vec![0, 1, 2, 3] },
            RawTaggedField { tag: 1, data: vec![0, 1] },
            RawTaggedField { tag: 4, data: vec![0, 1, 2, 3, 4, 5] }
        };

        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_unknown_tagged_fields(&mut cur, "test", &original_fields).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_fields = k_read_unknown_tagged_fields(&mut cur, "test").unwrap();

        assert_eq!(read_fields, original_fields);
    }

    #[test]
    fn test_serde_multiple_fields_wrong_order() {
        let original_fields = vec! {
            RawTaggedField { tag: 1, data: vec![0, 1] },
            RawTaggedField { tag: 0, data: vec![0, 1, 2, 3] },
            RawTaggedField { tag: 4, data: vec![0, 1, 2, 3, 4, 5] }
        };

        let mut cur = Cursor::new(Vec::<u8>::new());
        let error = k_write_unknown_tagged_fields(&mut cur, "test", &original_fields)
            .expect_err("must_be_error");
        assert_eq!(error.to_string(), "Invalid raw tag field list: tag 0 comes after tag 1, but is not higher than it.");
    }

    #[test]
    fn test_serde_multiple_fields_empty() {
        let original_fields = vec![];

        let mut cur = Cursor::new(Vec::<u8>::new());
        k_write_unknown_tagged_fields(&mut cur, "test", &original_fields).unwrap();

        cur.seek(SeekFrom::Start(0)).unwrap();
        let read_fields = k_read_unknown_tagged_fields(&mut cur, "test").unwrap();

        assert_eq!(read_fields, original_fields);
    }
}