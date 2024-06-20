#[cfg(test)]

use std::fmt::Debug;
use std::io::{Cursor, Seek, SeekFrom};
use crate::kafka_readable::KafkaReadable;
use crate::kafka_writable::KafkaWritable;

pub mod uuid;

pub(crate) fn test_serde<T>(data: T)
where
    T: KafkaReadable + KafkaWritable + Debug + PartialEq,
{
    let mut cur = Cursor::new(Vec::<u8>::new());
    data.write(&mut cur).unwrap();

    cur.seek(SeekFrom::Start(0)).unwrap();
    let data_read = T::read(&mut cur).unwrap();
    assert_eq!(data_read, data);
}
