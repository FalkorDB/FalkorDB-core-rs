use crate::binding::command_ctx_struct::CommandCtx;
use crate::binding::query_ctx_struct::{
    Graph, GraphContext, QueryCtx, _tlsQueryCtxKey, bolt_client_t, rax, EffectsBuffer,
    RedisModuleCtx, ResultSet, UndoLog, AST,
};
use std::ptr::{addr_of_mut, null_mut};

#[no_mangle]
extern "C" fn QueryCtx_Init() -> bool {
    unsafe { libc::pthread_key_create(addr_of_mut!(_tlsQueryCtxKey), None) == 0 }
}

#[no_mangle]
extern "C" fn QueryCtx_SetTLS(ctx: *mut QueryCtx) {
    unsafe {
        libc::pthread_setspecific(_tlsQueryCtxKey, ctx as *mut _);
    }
}

#[no_mangle]
extern "C" fn QueryCtx_RemoveFromTLS() {
    unsafe {
        libc::pthread_setspecific(_tlsQueryCtxKey, null_mut());
    }
}

#[no_mangle]
extern "C" fn QueryCtx_Free() {
    unsafe {
        let ctx = QueryCtx_GetQueryCtx_unchecked();
        assert!(!ctx.is_null());

        drop(Box::from_raw(ctx));
        libc::pthread_setspecific(_tlsQueryCtxKey, null_mut());
    }
}

#[no_mangle]
extern "C" fn QueryCtx_GetQueryCtx() -> *mut QueryCtx {
    QueryCtx::get_or_create_context()
}

#[no_mangle]
extern "C" fn QueryCtx_GetQueryCtx_unchecked() -> *mut QueryCtx {
    unsafe { libc::pthread_getspecific(_tlsQueryCtxKey) as *mut _ }
}

/// sets the global execution context
#[no_mangle]
extern "C" fn QueryCtx_SetGlobalExecutionCtx(cmd_ctx: *mut CommandCtx) {
    assert!(!cmd_ctx.is_null());

    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).set_global_execution_context(&mut *cmd_ctx);
    }
}

/// set the provided AST for access through the QueryCtx
#[no_mangle]
extern "C" fn QueryCtx_SetAST(ast: *mut AST) {
    // I think AST can be null since we don't assert this in the C code
    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).set_ast(ast);
    }
}

/// retrieve the AST
#[no_mangle]
extern "C" fn QueryCtx_GetAST() -> *mut AST {
    // I think AST can be null since we don't assert this in the C code
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_ast() }
}

/// Set the graph context
#[no_mangle]
extern "C" fn QueryCtx_SetGraphCtx(gc: *mut GraphContext) {
    assert!(!gc.is_null());

    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).set_graph_context(gc);
    }
}

/// retrieve the Graph Context
#[no_mangle]
extern "C" fn QueryCtx_GetGraphCtx() -> *mut GraphContext {
    // I think AST can be null since we don't assert this in the C code
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_graph_context() }
}

/// Retrieve the graph itself
#[no_mangle]
extern "C" fn QueryCtx_GetGraph() -> *mut Graph {
    // I think AST can be null since we don't assert this in the C code
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_graph() }
}

#[no_mangle]
extern "C" fn QueryCtx_GetUndoLog() -> UndoLog {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_undo_log() }
}

#[no_mangle]
extern "C" fn QueryCtx_Rollback() {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe {
        (*ctx).rollback();
    }
}

#[no_mangle]
extern "C" fn QueryCtx_AdvanceStage(ctx: *mut QueryCtx) {
    assert!(!ctx.is_null());
    unsafe {
        (*ctx).advance_stage();
    }
}

#[no_mangle]
extern "C" fn QueryCtx_ResetStage(ctx: *mut QueryCtx) {
    assert!(!ctx.is_null());
    unsafe {
        (*ctx).reset_stage();
    }
}

#[no_mangle]
extern "C" fn QueryCtx_SetUtilizedCache(
    ctx: *mut QueryCtx,
    utilized: bool,
) {
    assert!(!ctx.is_null());
    unsafe {
        (*ctx).set_utilized_cache(utilized);
    }
}

/// set the resultset
#[no_mangle]
extern "C" fn QueryCtx_SetResultSet(result_set: *mut ResultSet) {
    assert!(!result_set.is_null());

    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).set_result_set(result_set);
    }
}

#[no_mangle]
extern "C" fn QueryCtx_GetResultSet() -> *mut ResultSet {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_result_set() }
}

/// set the parameters map
#[no_mangle]
extern "C" fn QueryCtx_SetParams(rax: *mut rax) {
    assert!(!rax.is_null());

    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).set_params(rax);
    }
}

/// set the parameters map
#[no_mangle]
extern "C" fn QueryCtx_GetParams() -> *mut rax {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_params() }
}

#[no_mangle]
extern "C" fn QueryCtx_GetEffectsBuffer() -> *mut EffectsBuffer {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_effects_buffer() }
}

#[no_mangle]
extern "C" fn QueryCtx_GetRedisModuleCtx() -> *mut RedisModuleCtx {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_redis_module_context() }
}

#[no_mangle]
extern "C" fn QueryCtx_GetBoltClient() -> *mut bolt_client_t {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_bolt_client() }
}

#[no_mangle]
extern "C" fn QueryCtx_GetRuntime() -> f64 {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).get_runtime() }
}

// print the current query
#[no_mangle]
extern "C" fn QueryCtx_PrintQuery() {
    let ctx = QueryCtx::get_or_create_context();
    unsafe {
        (*ctx).print_query();
    }
}

#[no_mangle]
extern "C" fn QueryCtx_Replicate(ctx: *mut QueryCtx) {
    assert!(!ctx.is_null());
    unsafe {
        (*ctx).replicate();
    }
}

// starts a locking flow before commiting changes
// Locking flow:
// 1. lock GIL
// 2. open key with `write` flag
// 3. graph R/W lock with write flag
// since 2PL protocal is implemented, the method returns true if
// it managed to achieve locks in this call or a previous call
// in case that the locks are already locked, there will be no attempt to lock
// them again this method returns false if the key has changed
// from the current graph, and sets the relevant error message
#[no_mangle]
extern "C" fn QueryCtx_LockForCommit() -> bool {
    let ctx = QueryCtx::get_or_create_context();
    unsafe { (*ctx).lock_for_commit() }
}

// starts an ulocking flow and notifies Redis after commiting changes
// the only writer which allow to perform the unlock and commit (replicate)
// is the last_writer the method get an OpBase and compares it to
// the last writer, if they are equal then the commit and unlock flow will start
// Unlocking flow:
// 1. replicate
// 2. unlock graph R/W lock
// 3. close key
// 4. unlock GIL
#[no_mangle]
extern "C" fn QueryCtx_UnlockCommit() {
    let ctx = QueryCtx_GetQueryCtx_unchecked();
    assert!(!ctx.is_null());
    unsafe { (*ctx).unlock_for_commit() }
}
