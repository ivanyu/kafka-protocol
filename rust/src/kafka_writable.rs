use std::io::{Result, Write};
use byteorder::{BigEndian, WriteBytesExt};
use uuid::Uuid;

pub(crate) trait KafkaWritable: Sized {
    fn write(&self, output: &mut impl Write) -> Result<()>;
}

impl KafkaWritable for bool {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        if *self {
            output.write_i8(1)
        } else {
            output.write_i8(0)
        }
    }
}

impl KafkaWritable for i8 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_i8(*self)
    }
}

impl KafkaWritable for u16 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_u16::<BigEndian>(*self)
    }
}

impl KafkaWritable for i16 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_i16::<BigEndian>(*self)
    }
}

impl KafkaWritable for u32 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_u32::<BigEndian>(*self)
    }
}

impl KafkaWritable for i32 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_i32::<BigEndian>(*self)
    }
}

impl KafkaWritable for i64 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_i64::<BigEndian>(*self)
    }
}

impl KafkaWritable for f64 {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_f64::<BigEndian>(*self)
    }
}

impl KafkaWritable for Uuid {
    #[inline]
    fn write(&self, output: &mut impl Write) -> Result<()> {
        output.write_u128::<BigEndian>(self.as_u128())
    }
}
