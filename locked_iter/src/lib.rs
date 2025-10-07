pub mod has_iter;
pub mod util;

use std::{ops::Deref, sync::Arc};

use parking_lot::RwLock;

use crate::has_iter::HasIter;

pub trait GenRc<T>: Clone {
    fn new(t: T) -> Self;
}

impl<T> GenRc<T> for Arc<T> {
    fn new(t: T) -> Self {
        Arc::new(t)
    }
}

impl<T> GenRc<T> for std::rc::Rc<T> {
    fn new(t: T) -> Self {
        std::rc::Rc::new(t)
    }
}

pub struct LockedIter3<'data, 'iter, L: Barier>
where
    <L as Barier>::T: 'data,
{
    guard: L::DetachedGuard<'data>,
    iter: <<L as Barier>::T as HasIter>::Iter<'data>,
    _phantom: std::marker::PhantomData<&'iter ()>,
}

impl<'data, 'iter, L: Barier> LockedIter3<'data, 'iter, L> {
    pub(crate) fn new(guard: L::Guard<'data, L::T>) -> LockedIter3<'data, 'iter, L> {
        let (guard, t) = unsafe { L::DetachedGuard::detach_from::<L::T>(guard) };
        Self {
            guard,
            iter: t.iter(),
            _phantom: std::marker::PhantomData,
        }
    }
}

fn swap_lifetimes<'a, 'b, T>(t: &'a T) -> &'b T {
    unsafe { &*(t as *const T) }
}

impl<'data, 'iter, L: Barier> Iterator for LockedIter3<'data, 'iter, L>
where
    <L as Barier>::DetachedGuard<'data>: 'iter,
{
    type Item = LockedEntry3<'data, 'iter, L>;

    fn next(&mut self) -> Option<Self::Item> {
        let &mut LockedIter3 {
            ref guard,
            ref mut iter,
            ..
        } = self;
        let guard: &'iter <L as Barier>::DetachedGuard<'_> = swap_lifetimes(guard);
        iter.next().map(|t| LockedEntry3 { _guard: guard, t })
    }
}

pub struct LockedEntry3<'data, 'iter, L: Barier>
where
    <L as Barier>::T: 'data,
{
    _guard: &'iter L::DetachedGuard<'data>,
    t: <L::T as HasIter>::Item<'data>,
}

impl<'data, 'iter, L> Deref for LockedEntry3<'data, 'iter, L>
where
    L: Barier,
{
    type Target = <L::T as HasIter>::Item<'data>;

    fn deref(&self) -> &Self::Target {
        &self.t
    }
}

pub struct LockedIter2<'a, Rc, L: Barier>
where
    <L as Barier>::T: 'a,
    Rc: GenRc<L::DetachedGuard<'a>>,
{
    guard: Rc,
    iter: <<L as Barier>::T as HasIter>::Iter<'a>,
}

impl<'a, Rc: GenRc<L::DetachedGuard<'a>>, L: Barier> LockedIter2<'a, Rc, L> {
    pub(crate) fn new(guard: L::Guard<'a, L::T>) -> LockedIter2<'a, Rc, L> {
        let (guard, t) = unsafe { L::DetachedGuard::detach_from::<L::T>(guard) };
        Self {
            guard: Rc::new(guard),
            iter: t.iter(),
        }
    }
}

pub struct LockedEntry<Rc: GenRc<DG>, DG, A> {
    _guard: Rc,
    t: A,
    _phantom: std::marker::PhantomData<DG>,
}

impl<Rc: GenRc<DG>, DG, A> Deref for LockedEntry<Rc, DG, A> {
    type Target = A;

    fn deref(&self) -> &A {
        &self.t
    }
}

impl<'a, Rc: GenRc<<L as Barier>::DetachedGuard<'a>>, L: Barier> Iterator
    for LockedIter2<'a, Rc, L>
{
    type Item = LockedEntry<Rc, <L as Barier>::DetachedGuard<'a>, <L::T as HasIter>::Item<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|t| LockedEntry {
            _guard: self.guard.clone(),
            t,
            _phantom: std::marker::PhantomData,
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
    fn arc_locked_iter(&self) -> LockedIter2<'_, Arc<L::DetachedGuard<'_>>, L>;

    fn rc_locked_iter(&self) -> LockedIter2<'_, std::rc::Rc<L::DetachedGuard<'_>>, L>;

    fn locked_iter<'data, 'iter: 'data>(&'iter self) -> LockedIter3<'data, 'iter, L>;
}

impl<L: Barier> LockedIterExt<L> for L {
    fn arc_locked_iter(&self) -> LockedIter2<'_, Arc<L::DetachedGuard<'_>>, L> {
        LockedIter2::new(self.read_guard())
    }

    fn rc_locked_iter(&self) -> LockedIter2<'_, std::rc::Rc<L::DetachedGuard<'_>>, L> {
        LockedIter2::new(self.read_guard())
    }

    fn locked_iter<'data, 'iter: 'data>(&'iter self) -> LockedIter3<'data, 'iter, L> {
        LockedIter3::new(self.read_guard())
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
        = crate::util::rwlock::RwLockReadGuardDetached<'a, parking_lot::RawRwLock>
    where
        T: 'a;

    fn read_guard(&self) -> Self::Guard<'_, Self::T> {
        self.read()
    }
}

impl<'a> DetachableGuard<'a>
    for crate::util::rwlock::RwLockReadGuardDetached<'a, parking_lot::RawRwLock>
{
    type Guard<'b, T>
        = parking_lot::RwLockReadGuard<'b, T>
    where
        T: 'b + 'a;

    unsafe fn detach_from<T>(guard: Self::Guard<'a, T>) -> (Self, &'a T)
    where
        Self: Sized,
    {
        unsafe { crate::util::rwlock::RwLockReadGuardDetached::detach_from(guard) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn trait_adds_locked_iter_fn_vec() {
        let locked_vec = parking_lot::RwLock::new(vec![1, 2, 3]);

        let iter = locked_vec.arc_locked_iter();

        for (i, x) in iter.enumerate() {
            assert_eq!(**x, (i + 1) as i32);
        }
    }

    #[test]
    fn trait_adds_locked_iter_fn_array() {
        let locked_array: lock_api::RwLock<parking_lot::RawRwLock, Box<[i32]>> =
            parking_lot::RwLock::new(Box::new([1, 2, 3]));

        let iter = locked_array.rc_locked_iter();

        for (i, x) in iter.enumerate() {
            assert_eq!(**x, (i + 1) as i32);
        }
    }

    #[test]
    fn trait_adds_locked_iter_fn_array2() {
        let locked_array: lock_api::RwLock<parking_lot::RawRwLock, Box<[i32]>> =
            parking_lot::RwLock::new(Box::new([1, 2, 3]));

        {
            let mut iter = locked_array.locked_iter();

            let a = iter.next().unwrap();
            assert_eq!(**a, 1);
            drop(iter); // THIS SHOULD FAIL!
            println!("{:?}", **a)
        }
    }
}
