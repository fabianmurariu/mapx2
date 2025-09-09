use std::{ops::Deref, rc::Rc, sync::Arc};

use lock_api::RawMutex;

use super::util::MutexGuardDetached;

pub struct ArcEntry<'a, L: RawMutex, A> {
    _guard: Arc<MutexGuardDetached<'a, L>>,
    t: A,
}

impl<'a, L: RawMutex, A> ArcEntry<'a, L, A> {
    pub fn value(&self) -> &A {
        &self.t
    }

    pub(crate) fn new(guard: Arc<MutexGuardDetached<'a, L>>, t: A) -> Self {
        Self { _guard: guard, t }
    }
}

impl<'a, L: RawMutex, T: std::fmt::Debug> std::fmt::Debug for ArcEntry<'a, L, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcEntry")
            .field("t", &self.t)
            .finish_non_exhaustive()
    }
}

impl<L: RawMutex, A> Deref for ArcEntry<'_, L, A> {
    type Target = A;

    fn deref(&self) -> &A {
        self.value()
    }
}

pub struct RcEntry<'a, L: RawMutex, A> {
    _guard: Rc<MutexGuardDetached<'a, L>>,
    t: A,
}

impl<'a, L: RawMutex, A> RcEntry<'a, L, A> {
    pub fn value(&self) -> &A {
        &self.t
    }

    pub(crate) fn new(guard: Rc<MutexGuardDetached<'a, L>>, t: A) -> Self {
        Self { _guard: guard, t }
    }
}

impl<'a, L: RawMutex, T: std::fmt::Debug> std::fmt::Debug for RcEntry<'a, L, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcEntry")
            .field("t", &self.t)
            .finish_non_exhaustive()
    }
}

impl<L: RawMutex, A> Deref for RcEntry<'_, L, A> {
    type Target = A;

    fn deref(&self) -> &A {
        self.value()
    }
}