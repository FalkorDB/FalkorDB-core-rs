use crate::binding::query_ctx_struct::{
    bolt_client_t, GraphContext, RedisModuleBlockedClient, RedisModuleCtx, RedisModuleString,
};
use std::ffi::c_char;
use std::ptr::null_mut;
use std::sync::atomic::Ordering::SeqCst;

#[repr(u32)]
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ExecutorThread {
    Main = 0,
    Reader = 1,
    Writer = 2,
}

#[repr(C)]
#[derive(Debug)]
pub struct CommandCtx {
    pub query: *mut c_char,
    pub ctx: *mut RedisModuleCtx,
    pub command_name: *mut c_char,
    pub graph_ctx: *mut GraphContext,
    pub ref_count: std::sync::atomic::AtomicUsize,
    pub bc: *mut RedisModuleBlockedClient,
    pub replicated_command: bool,
    pub compact: bool,
    pub thread: ExecutorThread,
    pub timeout: i64,
    pub timeout_rw: bool,
    pub received_ts: u64,
    pub timer: [f64; 2],
    pub bolt_client: *mut bolt_client_t,
}

impl CommandCtx {
    pub fn new(
        ctx: *mut RedisModuleCtx,
        bc: *mut RedisModuleBlockedClient,
        cmd_name: *mut RedisModuleString,
        query: *mut RedisModuleString,
        graph_ctx: *mut GraphContext,
        thread: ExecutorThread,
        replicated_command: bool,
        compact: bool,
        timeout: i64,
        timeout_rw: bool,
        received_ts: u64,
        timer: *const f64,
        bolt_client: *mut bolt_client_t,
    ) -> Self {
        let mut command_ctx = Self {
            query: null_mut(),
            command_name: null_mut(),
            ctx,
            graph_ctx,
            ref_count: std::sync::atomic::AtomicUsize::new(1),
            bc,
            replicated_command,
            compact,
            thread,
            timeout,
            timeout_rw,
            received_ts,
            timer: [0.0; 2],
            bolt_client,
        };

        unsafe {
            command_ctx.timer[0] = *timer;
            command_ctx.timer[1] = *timer.offset(1);

            // TODO: create a util function to create owned cstrings
            if !cmd_name.is_null() {
                let c_ptr = redis_module::RedisModule_StringPtrLen.unwrap_unchecked()(
                    cmd_name as _,
                    null_mut(),
                );

                command_ctx.command_name = libc::strdup(c_ptr);

                // if let Ok(c_ref) = CStr::from_ptr(c_ptr)
                //     .to_str()
                //     .map_err(|err| eprintln!("Warning: invalid string provided: {err}"))
                //     .and_then(|c_ref| {
                //         CString::new(c_ref.as_bytes()).map_err(|err| {
                //             eprintln!("Warning: Got unexpected null character: {err}")
                //         })
                //     })
                // {
                //     command_ctx.command_name = Some(c_ref);
                // }
            }

            if !query.is_null() {
                let c_ptr = redis_module::RedisModule_StringPtrLen.unwrap_unchecked()(
                    query as _,
                    null_mut(),
                );

                command_ctx.query = libc::strdup(c_ptr);

                // if let Ok(c_ref) = CStr::from_ptr(c_ptr)
                //     .to_str()
                //     .map_err(|err| eprintln!("Warning: invalid string provided: {err}"))
                //     .and_then(|c_ref| {
                //         CString::new(c_ref.as_bytes()).map_err(|err| {
                //             eprintln!("Warning: Got unexpected null character: {err}")
                //         })
                //     })
                // {
                //     command_ctx.query = Some(c_ref);
                // }
            }
        }

        command_ctx
    }

    pub fn get_redis_context(&mut self) -> *mut RedisModuleCtx {
        if !self.ctx.is_null() {
            return self.ctx;
        }

        assert!(!self.bc.is_null());
        unsafe {
            self.ctx = redis_module::RedisModule_GetThreadSafeContext.unwrap_unchecked()(
                self.bc as *mut _,
            ) as *mut _;
        }

        self.ctx
    }

    #[inline]
    pub fn get_bolt_client(&self) -> *mut bolt_client_t {
        self.bolt_client
    }

    #[inline]
    pub fn is_replicated(&self) -> bool {
        self.replicated_command
    }

    #[inline]
    pub fn is_compact(&self) -> bool {
        self.compact
    }

    #[inline]
    pub fn set_thread_type(
        &mut self,
        thread_type: ExecutorThread,
    ) {
        self.thread = thread_type;
    }

    #[inline]
    pub fn get_thread_type(&self) -> ExecutorThread {
        self.thread
    }

    #[inline]
    pub fn get_timer(&self) -> &[f64; 2] {
        &self.timer
    }

    #[inline]
    pub fn get_timeout(&self) -> i64 {
        self.timeout
    }

    #[inline]
    pub fn get_timeout_readwrite(&self) -> bool {
        self.timeout_rw
    }

    #[no_mangle]
    pub fn get_received_timestamp(&self) -> u64 {
        self.received_ts
    }

    #[inline]
    pub fn get_blocking_client(&self) -> *mut RedisModuleBlockedClient {
        self.bc
    }

    #[inline]
    pub fn get_graph_context(&self) -> *mut GraphContext {
        self.graph_ctx
    }

    #[inline]
    pub fn get_command_name(&self) -> *const c_char {
        self.command_name
    }

    #[inline]
    pub fn get_query(&self) -> *const c_char {
        self.query
    }

    pub fn thread_safe_context_lock(&mut self) {
        assert!(!self.ctx.is_null());
        if self.bc.is_null() {
            return;
        }

        unsafe {
            redis_module::RedisModule_ThreadSafeContextLock.unwrap_unchecked()(self.ctx as *mut _);
        }
    }

    pub fn thread_safe_context_unlock(&mut self) {
        assert!(!self.ctx.is_null());
        if self.bc.is_null() {
            return;
        }

        unsafe {
            redis_module::RedisModule_ThreadSafeContextUnlock.unwrap_unchecked()(
                self.ctx as *mut _,
            );
        }
    }

    pub fn unblock_client(&mut self) {
        if self.bc.is_null() {
            return;
        }

        unsafe {
            RedisGraph_UnblockClient(self.bc);
            self.bc = null_mut();

            if !self.ctx.is_null() {
                redis_module::RedisModule_FreeThreadSafeContext.unwrap_unchecked()(
                    self.ctx as *mut _,
                );
                self.ctx = null_mut();
            }
        }
    }

    pub fn increment_refcount(&mut self) {
        self.ref_count.fetch_add(1, SeqCst);
    }

    pub fn check_drop(&mut self) -> bool {
        self.ref_count.fetch_sub(1, SeqCst) == 1
    }
}

impl Drop for CommandCtx {
    fn drop(&mut self) {
        assert!(self.bc.is_null());
        unsafe {
            if !self.query.is_null() {
                libc::free(self.query as _)
            }

            if !self.command_name.is_null() {
                libc::free(self.command_name as _)
            }
        }
    }
}

extern "C" {
    fn RedisGraph_UnblockClient(bc: *mut RedisModuleBlockedClient);
}
