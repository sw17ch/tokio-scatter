use std::{
    cell::UnsafeCell, sync::atomic::spin_loop_hint, sync::atomic::AtomicBool,
    sync::atomic::Ordering,
};

const UNLOCKED: bool = false;
const LOCKED: bool = true;

pub struct Spinner<T> {
    data: UnsafeCell<T>,
    state: AtomicBool,
}

impl<T> Spinner<T> {
    pub fn new(data: T) -> Self {
        Self {
            data: UnsafeCell::new(data),
            state: AtomicBool::new(UNLOCKED),
        }
    }

    pub fn lock(&self) -> SpinnerGuard<'_, T> {
        loop {
            let state = self
                .state
                .compare_and_swap(UNLOCKED, LOCKED, Ordering::AcqRel);
            if state == UNLOCKED {
                // We acquired the lock.
                return SpinnerGuard { spinner: self };
            } else {
                // We haven't yet acquired the lock.
                spin_loop_hint();
            }
        }
    }
}

pub struct SpinnerGuard<'s, T> {
    spinner: &'s Spinner<T>,
}

impl<'s, T> Drop for SpinnerGuard<'s, T> {
    fn drop(&mut self) {
        debug_assert_eq!(LOCKED, self.spinner.state.load(Ordering::Relaxed));
        self.spinner.state.store(UNLOCKED, Ordering::Release);
    }
}

impl<'s, T> SpinnerGuard<'s, T> {
    pub fn get(&mut self) -> &'s mut T {
        unsafe { &mut *self.spinner.data.get() }
    }
}

unsafe impl<T> Sync for Spinner<T> where T: Sync {}
