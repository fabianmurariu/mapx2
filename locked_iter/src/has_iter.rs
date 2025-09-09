use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
    sync::Arc,
};

pub trait HasIter {
    type Item<'a>
    where
        Self: 'a;
    type Iter<'a>: Iterator<Item = Self::Item<'a>>
    where
        Self::Item<'a>: 'a,
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
}

impl<T> HasIter for Vec<T> {
    type Item<'a>
        = &'a T
    where
        T: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }
}

impl<T> HasIter for Box<[T]> {
    type Item<'a>
        = &'a T
    where
        T: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_ref().iter()
    }
}

impl<T> HasIter for Arc<[T]> {
    type Item<'a>
        = &'a T
    where
        T: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_ref().iter()
    }
}

impl<T> HasIter for Rc<[T]> {
    type Item<'a>
        = &'a T
    where
        T: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_ref().iter()
    }
}

impl<K, V> HasIter for BTreeMap<K, V> {
    type Item<'a>
        = (&'a K, &'a V)
    where
        K: 'a,
        V: 'a;
    type Iter<'a>
        = std::collections::btree_map::Iter<'a, K, V>
    where
        K: 'a,
        V: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl<V> HasIter for BTreeSet<V> {
    type Item<'a>
        = &'a V
    where
        V: 'a;
    type Iter<'a>
        = std::collections::btree_set::Iter<'a, V>
    where
        V: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl<K, V> HasIter for HashMap<K, V> {
    type Item<'a>
        = (&'a K, &'a V)
    where
        K: 'a,
        V: 'a;
    type Iter<'a>
        = std::collections::hash_map::Iter<'a, K, V>
    where
        K: 'a,
        V: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl<'a, T> HasIter for &'a [T] {
    type Item<'b>
        = &'b T
    where
        T: 'b,
        'a: 'b;
    type Iter<'b>
        = std::slice::Iter<'b, T>
    where
        T: 'b,
        'a: 'b;

    fn iter(&self) -> Self::Iter<'_> {
        self[..].iter()
    }
}
