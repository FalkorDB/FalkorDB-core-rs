/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{cell::UnsafeCell, ptr::null_mut};

use libc::{pthread_rwlock_t, PTHREAD_RWLOCK_INITIALIZER};

/// Wrap C rwlock as we can't use Rust RWLock.
/// Used to lock the graph.
pub struct CRWLock {
    rwlock: UnsafeCell<pthread_rwlock_t>,
}

impl CRWLock {
    pub fn new() -> Self {
        let res = CRWLock {
            rwlock: UnsafeCell::new(PTHREAD_RWLOCK_INITIALIZER),
        };
        unsafe {
            let res = libc::pthread_rwlock_init(res.rwlock.get(), null_mut());
            debug_assert!(res == 0, "pthread_rwlock_init failed");
        }
        res
    }

    pub fn acquire_read(&self) {
        unsafe {
            let res = libc::pthread_rwlock_rdlock(self.rwlock.get());
            debug_assert!(res == 0, "pthread_rwlock_rdlock failed");
        }
    }

    pub fn acquire_write(&self) {
        unsafe {
            let res = libc::pthread_rwlock_wrlock(self.rwlock.get());
            debug_assert!(res == 0, "pthread_rwlock_wrlock failed");
        }
    }

    pub fn release(&self) {
        unsafe {
            let res = libc::pthread_rwlock_unlock(self.rwlock.get());
            debug_assert!(res == 0, "pthread_rwlock_unlock failed");
        }
    }
}

impl Drop for CRWLock {
    fn drop(&mut self) {
        unsafe { libc::pthread_rwlock_destroy(self.rwlock.get()) };
    }
}
