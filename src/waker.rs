use core::task::{Context, RawWaker, RawWakerVTable, Waker};
use std::{mem::ManuallyDrop, sync::Arc};

use crate::spinner::Spinner;

// The goal is to invoke a different waker for each writer. This waker should
// register its ID number so that the poll can quickly determine which writers
// have work to be done.

pub(crate) struct IdList {
    ids: Spinner<Vec<u64>>,
}

impl Default for IdList {
    fn default() -> Self {
        Self::new()
    }
}

impl IdList {
    fn new() -> Self {
        Self {
            ids: Spinner::new(Vec::new()),
        }
    }

    pub(crate) fn push(&self, id: u64) {
        let mut guard = self.ids.lock();
        guard.get().push(id);
    }

    pub(crate) fn push_slice(&self, slice: &[u64]) {
        let mut guard = self.ids.lock();
        guard.get().extend_from_slice(slice)
    }

    /// Take all the accumulated IDs. The provided vector is cleared and swapped
    /// with the existing list. This can be used to avoid costly re-allocations
    /// while holding the spinlock.
    pub(crate) fn take(&self, other: &mut Vec<u64>) {
        other.clear();

        let mut guard = self.ids.lock();
        let this = guard.get();
        std::mem::swap(&mut *this, other);
    }
}

#[derive(Clone)]
pub(crate) struct WakerProxy {
    id: u64,
    ids: Arc<IdList>,
    outer: Waker,
}

fn vt_clone(data: *const ()) -> RawWaker {
    // This whole thing assumes that Arc values are _just_ pointers, and that
    // cloning an Arc only requires bumping the refcount, and then copying the
    // pointer.
    debug_assert_eq!(
        std::mem::size_of::<*const ()>(),
        std::mem::size_of::<Arc<()>>()
    );

    // Retain Arc, but don't touch refcount by wrapping in ManuallyDrop
    let arc = unsafe { ManuallyDrop::new(Arc::<WakerProxy>::from_raw(data as *const WakerProxy)) };

    // Now increase refcount, but don't drop new refcount either
    let arc_clone: ManuallyDrop<_> = arc.clone();

    // Drop the `ManuallyDrop` values to avoid unused variable warnings.
    drop(arc);
    drop(arc_clone);

    // Finally, construct a new RawWaker by copying the `data` pointer, and
    // passing a reference to the VTABLE.
    RawWaker::new(data, &VTABLE)
}

fn vt_wake(data: *const ()) {
    // Here, we convert the pointer to an Arc. This causes us to take ownrship
    // of the Arc.
    let arc = unsafe { Arc::from_raw(data as *const WakerProxy) };
    arc.ids.push(arc.id);
    Waker::wake_by_ref(&arc.outer)
}

fn vt_wake_by_ref(data: *const ()) {
    // Here, we convert the pointer to an Arc, but then wrap it in a
    // ManuallyDrop. This prevents us from taking any ownership of the
    // Arc while still being able to use the content.
    let manual_arc =
        unsafe { ManuallyDrop::new(Arc::<WakerProxy>::from_raw(data as *const WakerProxy)) };
    manual_arc.ids.push(manual_arc.id);
    Waker::wake_by_ref(&manual_arc.outer)
}

fn vt_drop(data: *const ()) {
    drop(unsafe { Arc::from_raw(data as *const WakerProxy) })
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake_by_ref, vt_drop);

pub(crate) fn proxy_waker(id: u64, ids: &Arc<IdList>, context: &mut Context) -> Waker {
    let p = Arc::new(WakerProxy {
        id,
        ids: ids.clone(),
        outer: context.waker().clone(),
    });
    let raw = Arc::into_raw(p);

    unsafe { Waker::from_raw(RawWaker::new(raw as *const _, &VTABLE)) }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_push_and_take_work() {
        let i = IdList::new();

        i.push(10);
        i.push(11);
        i.push(12);

        let mut v = Vec::new();
        i.take(&mut v);

        assert_eq!(10, v[0]);
        assert_eq!(11, v[1]);
        assert_eq!(12, v[2]);
    }
}
