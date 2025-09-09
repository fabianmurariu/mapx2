pub mod has_iter;
pub mod mutex;
pub mod rwlock;

use std::{ops::Deref, sync::Arc};

use parking_lot::RwLock;
pub use rwlock::LockedT;

use crate::has_iter::HasIter;

trait GenRc<T>: Clone {}

pub struct LockedIter2<'a, L: Barier>
where
    <L as Barier>::T: 'a,
{
    guard: Arc<L::DetachedGuard<'a>>,
    iter: <<L as Barier>::T as HasIter>::Iter<'a>,
}

impl<'a, L: Barier> LockedIter2<'a, L> {
    pub(crate) fn new(guard: L::Guard<'a, L::T>) -> LockedIter2<'a, L> {
        let (guard, t) = unsafe { L::DetachedGuard::detach_from::<L::T>(guard) };
        Self {
            guard: Arc::new(guard),
            iter: t.iter(),
        }
    }
}

pub struct ArcEntry<DG, A> {
    _guard: Arc<DG>,
    t: A,
}

impl<DG, A> Deref for ArcEntry<DG, A> {
    type Target = A;

    fn deref(&self) -> &A {
        &self.t
    }
}

impl<'a, L: Barier> Iterator for LockedIter2<'a, L> {
    type Item = ArcEntry<<L as Barier>::DetachedGuard<'a>, <L::T as HasIter>::Item<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|t| ArcEntry {
            _guard: self.guard.clone(),
            t,
        })
    }
}

pub trait Barier {
    type T: HasIter;
    type Guard<'a, A>
    where
        A: 'a;
    type DetachedGuard<'a>: DetachableGuard<'a, Guard<'a, Self::T> = Self::Guard<'a, Self::T>>
    where
        <Self as Barier>::T: 'a;

    fn read_guard(&self) -> Self::Guard<'_, Self::T>;
}

pub trait DetachableGuard<'a> {
    type Guard<'b, T>: Sized
    where
        T: 'b,
        T: 'a;
    /// # Safety
    ///
    unsafe fn detach_from<T>(guard: Self::Guard<'a, T>) -> (Self, &'a T)
    where
        Self: Sized;
}

pub trait LockedIterExt<L: Barier> {
    fn locked_iter(&self) -> LockedIter2<'_, L>;
}

impl<L: Barier> LockedIterExt<L> for L {
    fn locked_iter(&self) -> LockedIter2<'_, L> {
        LockedIter2::new(self.read_guard())
    }
}

impl<T> Barier for RwLock<T>
where
    T: HasIter,
{
    type T = T;
    type Guard<'a, A>
        = parking_lot::RwLockReadGuard<'a, A>
    where
        A: 'a;
    type DetachedGuard<'a>
        = crate::rwlock::util::RwLockReadGuardDetached<'a, parking_lot::RawRwLock>
    where
        T: 'a;

    fn read_guard(&self) -> Self::Guard<'_, Self::T> {
        self.read()
    }
}

impl<'a> DetachableGuard<'a>
    for crate::rwlock::util::RwLockReadGuardDetached<'a, parking_lot::RawRwLock>
{
    type Guard<'b, T>
        = parking_lot::RwLockReadGuard<'b, T>
    where
        T: 'b + 'a;

    unsafe fn detach_from<T>(guard: Self::Guard<'a, T>) -> (Self, &'a T)
    where
        Self: Sized,
    {
        unsafe { crate::rwlock::util::RwLockReadGuardDetached::detach_from(guard) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn trait_adds_locked_iter_fn_vec() {
        let locked_vec = parking_lot::RwLock::new(vec![1, 2, 3]);

        let iter = locked_vec.locked_iter();

        for (i, x) in iter.enumerate() {
            assert_eq!(**x, (i + 1) as i32);
        }
    }

    #[test]
    fn trait_adds_locked_iter_fn_array() {
        let locked_array: lock_api::RwLock<parking_lot::RawRwLock, Box<[i32]>> =
            parking_lot::RwLock::new(Box::new([1, 2, 3]));

        let iter = locked_array.locked_iter();

        for (i, x) in iter.enumerate() {
            assert_eq!(**x, (i + 1) as i32);
        }
    }
}
