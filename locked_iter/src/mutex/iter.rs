use std::{iter::FusedIterator, sync::Arc};

use lock_api::{MutexGuard, RawMutex};

use super::{entry::ArcEntry, util::MutexGuardDetached};
use crate::has_iter::HasIter;

pub(crate) struct LockedIter<'a, L: RawMutex, T: HasIter + 'a> {
    guard: Arc<MutexGuardDetached<'a, L>>,
    iter: T::Iter<'a>,
}

impl<'a, L: RawMutex, T: HasIter + 'a> LockedIter<'a, L, T> {
    pub(crate) fn new(guard: MutexGuard<'a, L, T>) -> LockedIter<'a, L, T> {
        let (guard, t) = unsafe { MutexGuardDetached::detach_from(guard) };
        Self {
            guard: guard.into(),
            iter: t.iter(),
        }
    }
}

impl<'a, L: RawMutex, T: HasIter + 'a> Iterator for LockedIter<'a, L, T> {
    type Item = ArcEntry<'a, L, T::Item<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|t| ArcEntry::new(self.guard.clone(), t))
    }
}

pub(crate) struct LockedIterRc<'a, L: RawMutex, T: HasIter + 'a> {
    guard: std::rc::Rc<MutexGuardDetached<'a, L>>,
    iter: T::Iter<'a>,
}

impl<'a, L: RawMutex, T: HasIter + 'a> LockedIterRc<'a, L, T> {
    pub(crate) fn new(guard: MutexGuard<'a, L, T>) -> LockedIterRc<'a, L, T> {
        let (guard, t) = unsafe { MutexGuardDetached::detach_from(guard) };
        Self {
            guard: std::rc::Rc::new(guard),
            iter: t.iter(),
        }
    }
}

impl<'a, L: RawMutex, T: HasIter + 'a> Iterator for LockedIterRc<'a, L, T> {
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

impl<'a, L: RawMutex, T: HasIter + 'a> ExactSizeIterator for LockedIter<'a, L, T>
where
    T::Iter<'a>: ExactSizeIterator,
{
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'a, L: RawMutex, T: HasIter + 'a> DoubleEndedIterator for LockedIterRc<'a, L, T>
where
    T::Iter<'a>: DoubleEndedIterator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|t| super::entry::RcEntry::new(self.guard.clone(), t))
    }
}

impl<'a, L: RawMutex, T: HasIter + 'a> FusedIterator for LockedIterRc<'a, L, T> where
    T::Iter<'a>: FusedIterator
{
}
