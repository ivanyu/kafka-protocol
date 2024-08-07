use std::fmt::Debug;
use proptest::{collection, option};
use proptest::prelude::*;
use proptest::prelude::Strategy;
use uuid::Uuid;
use crate::tagged_fields::RawTaggedField;

pub(crate) fn string() -> impl Strategy<Value=String> {
    "\"[0-9a-zA-Z]{0,10}\""
}

pub(crate) fn optional_string() -> impl Strategy<Value=Option<String>> {
    option::of(string())
}

pub(crate) fn bytes() -> impl Strategy<Value=Vec<u8>> {
    collection::vec(prop::num::u8::ANY, collection::size_range(0..10))
}

pub(crate) fn optional_bytes() -> impl Strategy<Value=Option<Vec<u8>>> {
    option::of(bytes())
}

pub(crate) fn vec<T>() -> impl Strategy<Value=Vec<T>>
where
    T: Arbitrary,
{
    collection::vec(any::<T>(), collection::size_range(0..10))
}

pub(crate) fn vec_elem<T>(element: impl Strategy<Value=T>) -> impl Strategy<Value=Vec<T>>
where
    T: Debug,
{
    collection::vec(element, collection::size_range(0..10))
}

pub(crate) fn optional_vec<T>() -> impl Strategy<Value=Option<Vec<T>>>
where
    T: Arbitrary,
{
    option::of(vec())
}

pub(crate) fn uuid() -> impl Strategy<Value=Uuid> {
    any::<u128>().prop_map(Uuid::from_u128)
}

pub(crate) fn unknown_tagged_fields() -> impl Strategy<Value=Vec<RawTaggedField>> {
    prop_oneof![
        Just(Vec::<RawTaggedField>::new()),
        bytes().prop_map(|data| vec![RawTaggedField{ tag: 999, data }]),
    ]
}
