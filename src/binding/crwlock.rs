/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::cell::Cell;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

thread_local! {
    static READ_GUARD: Cell<Option<RwLockReadGuard<'static, ()>>> = Cell::new(None);
    static WRITE_GUARD: Cell<Option<RwLockWriteGuard<'static, ()>>> = Cell::new(None);
}

/// Wrap parking_lot rwlock to promote access from C and direct access from Rust.
/// Used to lock the graph.
pub struct CRWLock {
    rwlock: RwLock<()>,
}
unsafe impl Send for CRWLock {}
unsafe impl Sync for CRWLock {}

impl CRWLock {
    pub fn new() -> Self {
        CRWLock {
            rwlock: RwLock::new(()),
        }
    }

    #[inline]
    pub fn acquire_read(&self) {
        let guard = self.rwlock.read();
        unsafe {
            READ_GUARD.set(Some(std::mem::transmute(guard)));
        }
    }

    #[inline]
    pub fn acquire_write(&self) {
        let guard = self.rwlock.write();
        unsafe {
            WRITE_GUARD.set(Some(std::mem::transmute(guard)));
        }
    }

    #[inline]
    pub fn release(&self) {
        WRITE_GUARD.take();
        READ_GUARD.take();
    }
}
