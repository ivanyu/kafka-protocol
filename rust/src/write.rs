use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use uuid::Uuid;

// #[inline]
// pub(crate) fn k_write_bool(output: &mut impl Write, value: bool) -> std::io::Result<()> {
//     if value {
//         k_write_i8(output, 1)
//     } else {
//         k_write_i8(output, 0)
//     }
// }
//
// #[inline]
// pub(crate) fn k_write_i8(output: &mut impl Write, value: i8) -> std::io::Result<()> {
//     output.write_i8(value)
// }
//
// #[inline]
// pub(crate) fn k_write_u16(output: &mut impl Write, value: u16) -> std::io::Result<()> {
//     output.write_u16::<BigEndian>(value)
// }
//
// #[inline]
// pub(crate) fn k_write_i16(output: &mut impl Write, value: i16) -> std::io::Result<()> {
//     output.write_i16::<BigEndian>(value)
// }
//
// #[inline]
// pub(crate) fn k_write_u32(output: &mut impl Write, value: u32) -> std::io::Result<()> {
//     output.write_u32::<BigEndian>(value)
// }
//
// #[inline]
// pub(crate) fn k_write_i32(output: &mut impl Write, value: i32) -> std::io::Result<()> {
//     output.write_i32::<BigEndian>(value)
// }
//
// #[inline]
// pub(crate) fn k_write_i64(output: &mut impl Write, value: i64) -> std::io::Result<()> {
//     output.write_i64::<BigEndian>(value)
// }
//
// #[inline]
// pub(crate) fn k_write_f64(output: &mut impl Write, value: f64) -> std::io::Result<()> {
//     output.write_f64::<BigEndian>(value)
// }
//
// // #[inline]
// // pub(crate) fn k_read_uuid(input: &mut impl Read) -> std::io::Result<Uuid> {
// //     input.read_u128::<BigEndian>().map(Uuid::from_u128)
// // }
