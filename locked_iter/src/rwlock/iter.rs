use std::{iter::FusedIterator, sync::Arc};

use lock_api::{RawRwLock, RwLockReadGuard};

use crate::has_iter::HasIter;
use super::{entry::ArcEntry, util::RwLockReadGuardDetached};

pub(crate) struct LockedIter<'a, L: RawRwLock, T: HasIter + 'a> {
    guard: Arc<RwLockReadGuardDetached<'a, L>>,
    iter: T::Iter<'a>,
}

impl<'a, L: RawRwLock, T: HasIter + 'a> LockedIter<'a, L, T> {
    pub(crate) fn new(guard: RwLockReadGuard<'a, L, T>) -> LockedIter<'a, L, T> {
        let (guard, t) = unsafe { RwLockReadGuardDetached::detach_from(guard) };
        Self {
            guard: guard.into(),
            iter: t.iter(),
        }
    }
}

impl<'a, L: RawRwLock, T: HasIter + 'a> Iterator for LockedIter<'a, L, T> {
    type Item = ArcEntry<'a, L, T::Item<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|t| ArcEntry::new(self.guard.clone(), t))
    }
}

pub(crate) struct LockedIterRc<'a, L: RawRwLock, T: HasIter + 'a> {
    guard: std::rc::Rc<RwLockReadGuardDetached<'a, L>>,
    iter: T::Iter<'a>,
}

impl<'a, L: RawRwLock, T: HasIter + 'a> LockedIterRc<'a, L, T> {
    pub(crate) fn new(guard: RwLockReadGuard<'a, L, T>) -> LockedIterRc<'a, L, T> {
        let (guard, t) = unsafe { RwLockReadGuardDetached::detach_from(guard) };
        Self {
            guard: std::rc::Rc::new(guard),
            iter: t.iter(),
        }
    }
}

impl<'a, L: RawRwLock, T: HasIter + 'a> Iterator for LockedIterRc<'a, L, T> {
    type Item = super::entry::RcEntry<'a, L, T::Item<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|t| super::entry::RcEntry::new(self.guard.clone(), t))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter
            .nth(n)
            .map(|t| super::entry::RcEntry::new(self.guard.clone(), t))
    }
}

impl<'a, L: RawRwLock, T: HasIter + 'a> ExactSizeIterator for LockedIter<'a, L, T>
where
    T::Iter<'a>: ExactSizeIterator,
{
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'a, L: RawRwLock, T: HasIter + 'a> DoubleEndedIterator for LockedIterRc<'a, L, T>
where
    T::Iter<'a>: DoubleEndedIterator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|t| super::entry::RcEntry::new(self.guard.clone(), t))
    }
}

impl<'a, L: RawRwLock, T: HasIter + 'a> FusedIterator for LockedIterRc<'a, L, T> where
    T::Iter<'a>: FusedIterator
{
}
