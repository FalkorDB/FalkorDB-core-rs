/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use parking_lot::{Mutex, MutexGuard};
use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;

/// Wrap parking_lot mutex for C usage while keep the rust code working directly with the underline mutex.
/// Used to lock the matrix only when we apply pending changes.
/// Here is how it should call from C (rust code):
/// ```compile_only
/// let m = CMutex::new();
/// m.lock();
/// // do something
/// m.unlock();
/// ```
/// Here is how it should call from Rust:
/// ```compile_only
/// let m = CMutex::new();
/// // if you need to use this mutex from multiple threads,
/// // you need to put m.mutex in an Arc and clone it for each thread.
/// let guard = m.mutex.lock();
/// // do something
/// // drop guard or let it go out of scope
/// drop(guard);
/// ```
pub struct CMutex {
    pub mutex: Mutex<()>,
    guard: UnsafeCell<Option<ManuallyDrop<MutexGuard<'static, ()>>>>,
}
unsafe impl Send for CMutex {}
unsafe impl Sync for CMutex {}

impl CMutex {
    pub fn new() -> Self {
        CMutex {
            mutex: Mutex::new(()),
            guard: UnsafeCell::new(None),
        }
    }

    #[inline]
    pub fn lock(&self) {
        let guard = self.mutex.lock();
        unsafe {
            *self.guard.get() = Some(ManuallyDrop::new(std::mem::transmute(guard)));
        }
    }
    #[inline]
    pub fn unlock(&self) {
        unsafe {
            if let Some(mut guard) = (*self.guard.get()).take() {
                ManuallyDrop::drop(&mut guard);
            }
        }
    }
}

impl Drop for CMutex {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if let Some(mut guard) = (*self.guard.get_mut()).take() {
                ManuallyDrop::drop(&mut guard);
            }
        }
    }
}
