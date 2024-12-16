/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::cell::Cell;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use thread_local::ThreadLocal;

enum Guard {
    Read(RwLockReadGuard<'static, ()>),
    Write(RwLockWriteGuard<'static, ()>),
    None,
}
unsafe impl Send for Guard {}

/// Wrap parking_lot rwlock to promote access from C and direct access from Rust.
/// Used to lock the graph.
pub struct CRWLock {
    rwlock: RwLock<()>,
    guard: ThreadLocal<Cell<Guard>>,
}
unsafe impl Send for CRWLock {}
unsafe impl Sync for CRWLock {}

impl CRWLock {
    pub fn new() -> Self {
        CRWLock {
            rwlock: RwLock::new(()),
            guard: ThreadLocal::new(),
        }
    }

    #[inline]
    pub fn acquire_read(&self) {
        let cell = self.guard.get_or(|| Cell::new(Guard::None));
        unsafe {
            cell.set(Guard::Read(std::mem::transmute(self.rwlock.read())));
        }
    }

    #[inline]
    pub fn acquire_write(&self) {
        let cell = self.guard.get_or(|| Cell::new(Guard::None));
        unsafe {
            cell.set(Guard::Write(std::mem::transmute(self.rwlock.write())));
        }
    }

    #[inline]
    pub fn release(&self) {
        if let Some(cell) = self.guard.get() {
            cell.set(Guard::None);
        }
    }
}
