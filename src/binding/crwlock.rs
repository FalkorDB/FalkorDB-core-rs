/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::ptr::null_mut;

use libc::{pthread_rwlock_t, PTHREAD_RWLOCK_INITIALIZER};

/// Wrap C rwlock as we can't use Rust RWLock.
/// Used to lock the graph.
pub struct CRWLock {
    rwlock: Box<pthread_rwlock_t>,
}

impl CRWLock {
    pub fn new() -> Self {
        let mut res = CRWLock {
            rwlock: Box::new(PTHREAD_RWLOCK_INITIALIZER),
        };
        unsafe {
            libc::pthread_rwlock_init(res.rwlock.as_mut(), null_mut());
        }
        res
    }

    pub fn acquire_read(&mut self) {
        unsafe {
            let res = libc::pthread_rwlock_rdlock(self.rwlock.as_mut());
            debug_assert!(res == 0, "pthread_rwlock_rdlock failed");
        }
    }

    pub fn acquire_write(&mut self) {
        unsafe {
            let res = libc::pthread_rwlock_wrlock(self.rwlock.as_mut());
            debug_assert!(res == 0, "pthread_rwlock_wrlock failed");
        }
    }

    pub fn release(&mut self) {
        unsafe {
            let res = libc::pthread_rwlock_unlock(self.rwlock.as_mut());
            debug_assert!(res == 0, "pthread_rwlock_unlock failed");
        }
    }
}

impl Drop for CRWLock {
    fn drop(&mut self) {
        unsafe { libc::pthread_rwlock_destroy(self.rwlock.as_mut()) };
    }
}
