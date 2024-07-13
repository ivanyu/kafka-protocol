use crate::readable_writable::{KafkaReadable, KafkaWritable};

pub trait ApiMessage : KafkaWritable + KafkaReadable {}

pub trait Header : ApiMessage {}

pub trait Request : ApiMessage {}

pub trait Response : ApiMessage {}

pub trait Data : ApiMessage {}
