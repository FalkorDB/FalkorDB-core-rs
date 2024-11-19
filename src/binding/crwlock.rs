/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::cell::UnsafeCell;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::mem::ManuallyDrop;

/// Wrap parking_lot rwlock to promote access from C and direct access from Rust.
/// Used to lock the graph.
pub struct CRWLock {
    pub rwlock: RwLock<()>,
    read_guards: Mutex<Vec<ManuallyDrop<RwLockReadGuard<'static, ()>>>>,
    write_guard: UnsafeCell<Option<ManuallyDrop<RwLockWriteGuard<'static, ()>>>>,
}
unsafe impl Send for CRWLock {}
unsafe impl Sync for CRWLock {}

impl CRWLock {
    pub fn new() -> Self {
        CRWLock {
            rwlock: RwLock::new(()),
            read_guards: Mutex::new(Vec::new()),
            write_guard: UnsafeCell::new(None),
        }
    }

    #[inline]
    pub fn acquire_read(&self) {
        let guard = self.rwlock.read();
        let static_guard = unsafe { std::mem::transmute(guard) };
        self.read_guards
            .lock()
            .push(ManuallyDrop::new(static_guard));
    }

    #[inline]
    pub fn acquire_write(&self) {
        let guard = self.rwlock.write();
        unsafe {
            *self.write_guard.get() = Some(ManuallyDrop::new(std::mem::transmute(guard)));
        }
    }

    #[inline]
    pub fn release(&self) {
        unsafe {
            if let Some(mut write_guard) = (*self.write_guard.get()).take() {
                ManuallyDrop::drop(&mut write_guard);
            } else if let Some(mut read_guard) = self.read_guards.lock().pop() {
                ManuallyDrop::drop(&mut read_guard);
            }
        }
    }
}

impl Drop for CRWLock {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if let Some(mut write_guard) = self.write_guard.get_mut().take() {
                ManuallyDrop::drop(&mut write_guard);
            }
            while let Some(mut read_guard) = self.read_guards.get_mut().pop() {
                ManuallyDrop::drop(&mut read_guard);
            }
        }
    }
}
