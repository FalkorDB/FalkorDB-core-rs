/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[repr(C)]
pub enum CRWGuard {
    Read(RwLockReadGuard<'static, ()>),
    Write(RwLockWriteGuard<'static, ()>),
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
    pub fn acquire_read(&self) -> CRWGuard {
        CRWGuard::Read(unsafe { std::mem::transmute(self.rwlock.read()) })
    }

    #[inline]
    pub fn acquire_write(&self) -> CRWGuard {
        CRWGuard::Write(unsafe { std::mem::transmute(self.rwlock.write()) })
    }
}
