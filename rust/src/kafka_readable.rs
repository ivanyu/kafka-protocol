use std::io::Read;
use byteorder::{BigEndian, ReadBytesExt};
use uuid::Uuid;

pub(crate) trait KafkaReadable: Sized {
    fn read(input: &mut impl Read) -> std::io::Result<Self>;
}

impl KafkaReadable for bool {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_i8().map(|v| v != 0)
    }
}

impl KafkaReadable for i8 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_i8()
    }
}

impl KafkaReadable for u16 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_u16::<BigEndian>()
    }
}

impl KafkaReadable for i16 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_i16::<BigEndian>()
    }
}

impl KafkaReadable for u32 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_u32::<BigEndian>()
    }
}

impl KafkaReadable for i32 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_i32::<BigEndian>()
    }
}

impl KafkaReadable for i64 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_i64::<BigEndian>()
    }
}

impl KafkaReadable for f64 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_f64::<BigEndian>()
    }
}

impl KafkaReadable for Uuid {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        input.read_u128::<BigEndian>().map(Uuid::from_u128)
    }
}

impl KafkaReadable for Option<Vec<u8>> {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        let str_len = input.read_i32::<BigEndian>()?;
        if str_len < 0 {
            Ok(None)
        } else {
            let mut str_buf = vec![0_u8; str_len as usize];
            input.read_exact(&mut str_buf)?;
            Ok(Some(str_buf))
        }
    }
}
