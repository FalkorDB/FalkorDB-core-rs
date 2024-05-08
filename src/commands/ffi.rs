use crate::binding::query_ctx_struct::{
    bolt_client_t, GraphContext, RedisModuleBlockedClient, RedisModuleCtx, RedisModuleString,
};
use crate::commands::command_ctx::{CommandCtx, ExecutorThread};
use std::ffi::c_char;

#[no_mangle]
pub extern "C" fn CommandCtx_New(
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
    timer: *mut f64,
    bolt_client: *mut bolt_client_t,
) -> *mut CommandCtx {
    Box::into_raw(Box::new(CommandCtx::new(
        ctx,
        bc,
        cmd_name,
        query,
        graph_ctx,
        thread,
        replicated_command,
        compact,
        timeout,
        timeout_rw,
        received_ts,
        timer,
        bolt_client,
    )))
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetRedisCtx(command_ctx: *mut CommandCtx) -> *mut RedisModuleCtx {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_redis_context() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetBoltClient(command_ctx: *const CommandCtx) -> *mut bolt_client_t {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_bolt_client() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_SetThreadType(
    command_ctx: *mut CommandCtx,
    thread_type: ExecutorThread,
) {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).set_thread_type(thread_type) }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetThreadType(command_ctx: *const CommandCtx) -> ExecutorThread {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_thread_type() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_IsCompact(command_ctx: *const CommandCtx) -> bool {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).is_compact() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_IsReplicated(command_ctx: *const CommandCtx) -> bool {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).is_replicated() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetBlockingClient(
    command_ctx: *const CommandCtx
) -> *mut RedisModuleBlockedClient {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_blocking_client() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetGraphContext(command_ctx: *const CommandCtx) -> *mut GraphContext {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_graph_context() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetCommandName(command_ctx: *const CommandCtx) -> *const c_char {
    assert!(!command_ctx.is_null());

    unsafe {
        (*command_ctx).get_command_name()
        // .map(|c_ref| c_ref.as_ptr())
        // .unwrap_or(null())
    }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetQuery(command_ctx: *const CommandCtx) -> *const c_char {
    assert!(!command_ctx.is_null());

    unsafe {
        (*command_ctx).get_query()
        // .map(|c_ref| c_ref.as_ptr())
        // .unwrap_or(null())
    }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetTimeout(command_ctx: *const CommandCtx) -> i64 {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_timeout() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetTimer(command_ctx: *const CommandCtx) -> *const f64 {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_timer().as_ptr() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetReceivedTimestamp(command_ctx: *const CommandCtx) -> u64 {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_received_timestamp() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_GetTimeoutReadWrite(command_ctx: *const CommandCtx) -> bool {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).get_timeout_readwrite() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_ThreadSafeContextLock(command_ctx: *mut CommandCtx) {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).thread_safe_context_lock() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_ThreadSafeContextUnlock(command_ctx: *mut CommandCtx) {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).thread_safe_context_unlock() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_UnblockClient(command_ctx: *mut CommandCtx) {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).unblock_client() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_Incref(command_ctx: *mut CommandCtx) {
    assert!(!command_ctx.is_null());

    unsafe { (*command_ctx).increment_refcount() }
}

#[no_mangle]
pub extern "C" fn CommandCtx_Free(command_ctx: *mut CommandCtx) {
    assert!(!command_ctx.is_null());

    unsafe {
        let command_ctx = &mut (*command_ctx);
        if command_ctx.check_drop() {
            drop(Box::from_raw(command_ctx));
        }
    }
}
