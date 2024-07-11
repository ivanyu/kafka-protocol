use std::io::{Read, Write};
use std::io;

pub(crate) trait KafkaReadable: Sized {
    fn read(input: &mut impl Read) -> io::Result<Self>;
}

pub(crate) trait KafkaWritable: Sized {
    fn write(&self, output: &mut impl Write) -> io::Result<()>;
}
