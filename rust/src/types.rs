use std::fmt::Debug;

#[cfg(test)] use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct BaseRecords {
    // Not yet implemented
}
