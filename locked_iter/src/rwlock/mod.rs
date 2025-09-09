use lock_api::{RawRwLock, RwLock};

use crate::has_iter::HasIter;
use self::{entry::ArcEntry, iter::LockedIter, iter::LockedIterRc};

pub mod entry;
pub mod iter;
pub(crate) mod util;

pub struct LockedT<L, T> {
    locked: RwLock<L, T>,
}

impl<L: RawRwLock, T: HasIter> LockedT<L, T> {
    pub fn new(t: T) -> Self {
        Self {
            locked: RwLock::new(t),
        }
    }

    pub fn arc_iter(&self) -> impl Iterator<Item = ArcEntry<'_, L, T::Item<'_>>> {
        let guard = self.locked.read();
        LockedIter::new(guard)
    }

    pub fn rc_iter(&self) -> impl Iterator<Item = entry::RcEntry<'_, L, T::Item<'_>>> {
        let guard = self.locked.read();
        LockedIterRc::new(guard)
    }
}

#[cfg(test)]
mod test {
    use super::LockedT;
    use std::collections::BTreeMap;

    #[test]
    fn test_locked_iter() {
        let mut btree = BTreeMap::new();
        btree.insert("a".to_owned(), 1);
        btree.insert("b".to_owned(), 2);
        btree.insert("c".to_owned(), 3);

        let locked = LockedT::<parking_lot::RawRwLock, _>::new(btree);

        let mut iter = locked.arc_iter();

        let a = iter.next();
        assert_eq!(a.as_deref(), Some((&"a".to_owned(), &1)).as_ref());

        drop(iter);

        println!("{a:?}", a = a.unwrap().value())
    }
}