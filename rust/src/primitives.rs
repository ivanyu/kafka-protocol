use std::io;
use std::io::{Read, Result, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use uuid::Uuid;
use paste::paste;
use crate::readable_writable::{KafkaReadable, KafkaWritable};

macro_rules! impl_num_8bit {
    ($type:ty) => {
        impl KafkaReadable for $type {
            #[inline]
            fn read(input: &mut impl Read) -> Result<Self> {
                paste! {
                   input.[<read_ $type>]()
                }
            }
        }

        impl KafkaWritable for $type {
            #[inline]
            fn write(&self, output: &mut impl Write) -> Result<()> {
                paste! {
                   output.[<write_ $type>](*self)
                }
            }
        }
    };
}

impl_num_8bit!(i8);
impl_num_8bit!(u8);

macro_rules! impl_num {
    ($type:ty) => {
        impl KafkaReadable for $type {
            #[inline]
            fn read(input: &mut impl Read) -> Result<Self> {
                paste! {
                   input.[<read_ $type>]::<BigEndian>()
                }
            }
        }

        impl KafkaWritable for $type {
            #[inline]
            fn write(&self, output: &mut impl Write) -> Result<()> {
                paste! {
                   output.[<write_ $type>]::<BigEndian>(*self)
                }
            }
        }
    };
}

impl_num!(u16);
impl_num!(i16);
impl_num!(u32);
impl_num!(i32);
impl_num!(i64);
impl_num!(f64);

impl KafkaReadable for bool {
    fn read(input: &mut impl Read) -> io::Result<Self> {
        input.read_i8().map(|v| v != 0)
    }
}

impl KafkaWritable for bool {
    #[inline]
    fn write(&self, output: &mut impl Write) -> io::Result<()> {
        if *self {
            output.write_i8(1)
        } else {
            output.write_i8(0)
        }
    }
}

impl KafkaReadable for Uuid {
    fn read(input: &mut impl Read) -> io::Result<Self> {
        input.read_u128::<BigEndian>().map(Uuid::from_u128)
    }
}

impl KafkaWritable for Uuid {
    #[inline]
    fn write(&self, output: &mut impl Write) -> io::Result<()> {
        output.write_u128::<BigEndian>(self.as_u128())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use rstest::rstest;
    use uuid::uuid;

    use super::*;

    macro_rules! test {
        ($type:ty, $($c:tt),+) => {
            paste! {
                #[rstest]
                $(#[case($c)])+
                fn [<test_ $type:lower>](#[case] original_value: $type) {
                    let mut cur = Cursor::new(Vec::<u8>::new());
                    original_value.write(&mut cur).unwrap();
                    cur.seek(SeekFrom::Start(0)).unwrap();
                    let read_value = $type::read(&mut cur).unwrap();
                    assert_eq!(read_value, original_value);
                }
            }
        };
    }

    test!(bool, true, false);
    test!(u8, 0, 1);
    test!(i8, {-1}, 0, 1);
    test!(u16, 0, 1);
    test!(i16, {-1}, 0, 1);
    test!(u32, 0, 1);
    test!(i32, {-1}, 0, 1);
    test!(i64, {-1}, 0, 1);
    test!(f64, {-1.0}, 0.0, 1.0);
    test!(Uuid,
        {uuid!("00000000-0000-0000-0000-000000000000")},
        {uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")}
    );
}
