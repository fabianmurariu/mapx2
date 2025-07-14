use std::hash::RandomState;

use crate::{Buffers, ByteStore};

/// This is a open address hash map implementation,
/// it takes any &[u8] as key and value.
/// It is designed to be used with a backing store that implements
/// `ByteStore` trait, allowing for flexible storage options.
/// the `ByteStore` is not used directly instead we rely on `Buffers`
/// which is technically a `Vec<Box<[u8]>>` but backed by a `ByteStore` trait.
/// The hash function and equality function are provided as closures
pub struct OpenHashMap<
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    HFn: Fn(&[u8]) -> usize,
    EqFn: Fn(&[u8]) -> usize,
    KBs: ByteStore,
    VBs: ByteStore,
    S = RandomState,
> {
    keys: Buffers<KBs>,
    values: Buffers<VBs>,
    hash_fn: HFn,
    eq_fn: EqFn,
    _marker: std::marker::PhantomData<(K, V, S)>,
}
