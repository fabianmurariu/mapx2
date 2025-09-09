use core::{mem, ptr};
use std::{marker::PhantomData, mem::ManuallyDrop};

use lock_api::{RawMutex, MutexGuard};

pub const fn ptr_size_bits() -> usize {
    mem::size_of::<usize>() * 8
}

pub fn map_in_place_2<T, U, F: FnOnce(U, T) -> T>((k, v): (U, &mut T), f: F) {
    unsafe {
        // # Safety
        //
        // If the closure panics, we must abort otherwise we could double drop `T`
        let promote_panic_to_abort = AbortOnPanic;

        ptr::write(v, f(k, ptr::read(v)));

        // If we made it here, the calling thread could have already have panicked, in which case
        // We know that the closure did not panic, so don't bother checking.
        std::mem::forget(promote_panic_to_abort);
    }
}

struct AbortOnPanic;

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort()
        }
    }
}

/// A [`MutexGuard`], without the data
pub(crate) struct MutexGuardDetached<'a, R: RawMutex> {
    lock: &'a R,
    _marker: PhantomData<R::GuardMarker>,
}

impl<R: RawMutex> Drop for MutexGuardDetached<'_, R> {
    fn drop(&mut self) {
        // Safety: A MutexGuardDetached always holds an exclusive lock.
        unsafe {
            self.lock.unlock();
        }
    }
}

impl<'a, R: RawMutex> MutexGuardDetached<'a, R> {
    /// Separates the data from the [`MutexGuard`]
    ///
    /// # Safety
    ///
    /// The data must not outlive the detached guard
    pub(crate) unsafe fn detach_from<T>(guard: MutexGuard<'a, R, T>) -> (Self, &'a T) {
        let mutex = MutexGuard::mutex(&ManuallyDrop::new(guard));

        // Safety: There will be no concurrent access as we are "forgetting" the existing guard,
        // with the safety assumption that the caller will not drop the new detached guard early.
        let data = unsafe { &*mutex.data_ptr() };
        let guard = MutexGuardDetached {
            // Safety: We are imitating the original MutexGuard. It's the callers
            // responsibility to not drop the guard early.
            lock: unsafe { mutex.raw() },
            _marker: PhantomData,
        };
        (guard, data)
    }
}