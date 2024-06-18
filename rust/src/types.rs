use std::io::Read;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::read::{k_read_i32, k_read_i64, k_read_i8, k_read_uuid};

#[derive(Serialize, Deserialize)]
pub struct BaseRecords {
    // Not yet implemented
}

pub(crate) trait ReadableStruct: Sized {
    fn read(input: &mut impl Read) -> std::io::Result<Self>;
}

impl ReadableStruct for i8 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        k_read_i8(input)
    }
}

impl ReadableStruct for i32 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        k_read_i32(input)
    }
}

impl ReadableStruct for i64 {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        k_read_i64(input)
    }
}

impl ReadableStruct for Uuid {
    fn read(input: &mut impl Read) -> std::io::Result<Self> {
        k_read_uuid(input)
    }
}
