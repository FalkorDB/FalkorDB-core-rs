/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{mem::MaybeUninit, ptr::null_mut};

use libc::pthread_mutex_t;

/// Wrap C mutex as we can't use Rust Mutex.
/// Used to lock the matrix only when we apply pending changes.
pub struct CMutex {
    mutex: pthread_mutex_t,
}

impl CMutex {
    pub fn new() -> Self {
        unsafe {
            let mut mutex = MaybeUninit::uninit();
            libc::pthread_mutex_init(mutex.as_mut_ptr(), null_mut());
            Self {
                mutex: mutex.assume_init(),
            }
        }
    }

    pub fn lock(&mut self) {
        unsafe {
            libc::pthread_mutex_lock(&mut self.mutex);
        }
    }

    pub fn unlock(&mut self) {
        unsafe {
            libc::pthread_mutex_unlock(&mut self.mutex);
        }
    }
}

impl Drop for CMutex {
    fn drop(&mut self) {
        unsafe { libc::pthread_mutex_destroy(&mut self.mutex) };
    }
}
