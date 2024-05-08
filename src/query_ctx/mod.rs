mod ffi;
use crate::binding::query_ctx_struct::{
    GraphContext, QueryCtx, QueryCtx_GlobalExecCtx, QueryCtx_InternalExecCtx, QueryCtx_QueryData,
    QueryStats, ResultSet, _tlsQueryCtxKey, bolt_client_t, rax, EffectsBuffer, Graph,
    QueryExecutionStatus_QueryExecutionStatus_TIMEDOUT,
    QueryExecutionTypeFlag_QueryExecutionTypeFlag_WRITE, QueryStage_QueryStage_EXECUTING,
    QueryStage_QueryStage_REPORTING, QueryStage_QueryStage_WAITING, RedisModuleCtx, AST,
};
use crate::commands::command_ctx::CommandCtx;
use crate::errors::{ErrorCtx_RaiseRuntimeException, ErrorCtx_SetError};
use crate::undo_log::ffi::UndoLog_Rollback;
use crate::undo_log::undo_log::UndoLog;
use std::ffi::{c_char, c_void, CStr};
use std::ptr::{null, null_mut};

impl QueryCtx {
    pub fn new() -> Self {
        Self {
            stats: QueryStats {
                timer: [0.0; 2],
                received_ts: 0,
                durations: [0.0; 3],
                parameterized: false,
                utilized_cache: false,
            },
            gc: null_mut(),
            undo_log: null_mut(),
            stage: 0,
            status: 0,
            flags: 0,
            effects_buffer: null_mut(),
            query_data: QueryCtx_QueryData {
                ast: null_mut(),
                params: null_mut(),
                query: null_mut(),
                query_no_params: null_mut(),
            },
            global_exec_ctx: QueryCtx_GlobalExecCtx {
                redis_ctx: null_mut(),
                bc: null_mut(),
                bolt_client: null_mut(),
                command_name: null_mut(),
            },
            internal_exec_ctx: QueryCtx_InternalExecCtx {
                key: null_mut(),
                result_set: null_mut(),
                locked_for_commit: false,
            },
        }
    }

    #[inline]
    pub fn set_global_execution_context(
        &mut self,
        command_ctx: &mut CommandCtx,
    ) {
        self.gc = command_ctx.graph_ctx as *mut _;
        self.query_data.query = command_ctx.get_query();
        // .map(|c_ref| c_ref.as_ptr())
        // .unwrap_or(null());
        self.global_exec_ctx.bc = command_ctx.get_blocking_client() as *mut _;
        self.global_exec_ctx.redis_ctx = command_ctx.get_redis_context() as *mut _;
        self.global_exec_ctx.bolt_client = command_ctx.get_bolt_client() as *mut _;
        self.global_exec_ctx.command_name = command_ctx.get_command_name();
        // .map(|c_ref| c_ref.as_ptr())
        // .unwrap_or(null());

        // copy command's timer
        self.stats.timer = command_ctx.timer;

        // received timestamp (epoch time)
        self.stats.received_ts = command_ctx.received_ts;
    }

    #[inline]
    pub fn set_ast(
        &mut self,
        ast: *mut AST,
    ) {
        self.query_data.ast = ast;
    }

    #[inline]
    pub fn get_ast(&mut self) -> *mut AST {
        self.query_data.ast
    }

    #[inline]
    pub fn set_graph_context(
        &mut self,
        gc: *mut GraphContext,
    ) {
        self.gc = gc;
    }

    #[inline]
    pub fn get_graph_context(&mut self) -> *mut GraphContext {
        self.gc
    }

    #[inline]
    pub fn get_graph(&mut self) -> *mut Graph {
        unsafe { (*self.gc).g }
    }

    #[inline]
    pub fn set_result_set(
        &mut self,
        result_set: *mut ResultSet,
    ) {
        self.internal_exec_ctx.result_set = result_set;
    }

    #[inline]
    pub fn get_result_set(&mut self) -> *mut ResultSet {
        self.internal_exec_ctx.result_set
    }

    #[inline]
    pub fn set_params(
        &mut self,
        rax: *mut rax,
    ) {
        self.query_data.params = rax;
    }

    #[inline]
    pub fn get_params(&mut self) -> *mut rax {
        self.query_data.params
    }

    #[inline]
    pub fn print_query(&self) {
        unsafe {
            println!(
                "{}",
                CStr::from_ptr(self.query_data.query).to_string_lossy()
            );
        }
    }

    #[inline]
    pub fn set_utilized_cache(
        &mut self,
        utilized: bool,
    ) {
        self.stats.utilized_cache = utilized;
    }

    #[inline]
    fn get_undo_log(&mut self) -> *mut UndoLog {
        if self.undo_log.is_null() {
            self.undo_log = Box::into_raw(Box::new(UndoLog::new())) as *mut _;
        }

        self.undo_log as *mut _
    }

    #[inline]
    fn rollback(&mut self) {
        unsafe {
            Graph_ResetReservedNode((*self.gc).g);

            if self.undo_log.is_null() {
                return;
            }

            UndoLog_Rollback(self.undo_log as *mut _, self.gc as *mut _);
            self.undo_log = null_mut();
        }
    }

    fn update_stage_duration(&mut self) {
        unsafe {
            let ms = simple_toc(self.stats.timer.as_ptr()) * 1000.0;
            simple_tic(self.stats.timer.as_mut_ptr());

            self.stats.durations[self.stage as usize] += ms;
        }
    }

    fn advance_stage(&mut self) {
        assert!(self.stage <= QueryStage_QueryStage_REPORTING);

        if self.stage == QueryStage_QueryStage_REPORTING {
            unsafe {
                GraphContext_LogQuery(
                    self.gc,
                    self.stats.received_ts,
                    self.stats.durations[QueryStage_QueryStage_WAITING as usize],
                    self.stats.durations[QueryStage_QueryStage_EXECUTING as usize],
                    self.stats.durations[QueryStage_QueryStage_REPORTING as usize],
                    self.stats.parameterized,
                    self.stats.utilized_cache,
                    (self.flags & QueryExecutionTypeFlag_QueryExecutionTypeFlag_WRITE) != 0,
                    self.status == QueryExecutionStatus_QueryExecutionStatus_TIMEDOUT,
                    self.query_data.query,
                );
            }
        }

        self.stage += 1;
    }

    #[inline]
    fn reset_stage(&mut self) {
        assert_eq!(self.stage, QueryStage_QueryStage_EXECUTING);

        self.update_stage_duration();

        self.stage = QueryStage_QueryStage_WAITING;
    }

    #[inline]
    fn get_effects_buffer(&mut self) -> *mut EffectsBuffer {
        if self.effects_buffer.is_null() {
            unsafe {
                self.effects_buffer = EffectsBuffer_New();
            }
        }

        self.effects_buffer
    }

    #[inline]
    fn get_redis_module_context(&mut self) -> *mut RedisModuleCtx {
        self.global_exec_ctx.redis_ctx
    }

    #[inline]
    fn get_bolt_client(&mut self) -> *mut bolt_client_t {
        self.global_exec_ctx.bolt_client
    }

    #[inline]
    fn get_runtime(&self) -> f64 {
        self.stats.durations[QueryStage_QueryStage_EXECUTING as usize]
            + self.stats.durations[QueryStage_QueryStage_REPORTING as usize]
    }

    fn replicate(&mut self) {
        let graph_context = self.gc;
        let redis_context = self.global_exec_ctx.redis_ctx;

        unsafe {
            redis_module::raw::RedisModule_Replicate.unwrap_unchecked()(
                redis_context as *mut _,
                self.global_exec_ctx.command_name,
                c"cc!".as_ptr(),
                (*graph_context).graph_name,
                self.query_data.query,
            );
        }
    }

    fn thread_safe_context_lock(&mut self) {
        if self.global_exec_ctx.bc.is_null() {
            return;
        }
        unsafe {
            redis_module::raw::RedisModule_ThreadSafeContextLock.unwrap_unchecked()(
                self.global_exec_ctx.bc as *mut _,
            );
        }
    }

    fn thread_safe_context_unlock(&mut self) {
        if self.global_exec_ctx.bc.is_null() {
            return;
        }
        unsafe {
            redis_module::raw::RedisModule_ThreadSafeContextUnlock.unwrap_unchecked()(
                self.global_exec_ctx.bc as *mut _,
            );
        }
    }

    pub fn lock_for_commit(&mut self) -> bool {
        if self.internal_exec_ctx.locked_for_commit {
            return true;
        }

        let redis_ctx = self.global_exec_ctx.redis_ctx;
        let gc = self.gc;
        let graph_id = unsafe {
            redis_module::RedisModule_CreateString.unwrap_unchecked()(
                redis_ctx as *mut _,
                (*gc).graph_name,
                libc::strlen((*gc).graph_name),
            )
        };

        self.thread_safe_context_lock();

        unsafe {
            let key = redis_module::RedisModule_OpenKey.unwrap_unchecked()(
                redis_ctx as *mut _,
                graph_id,
                redis_module::REDISMODULE_WRITE as i32,
            );
            redis_module::RedisModule_FreeString.unwrap_unchecked()(redis_ctx as *mut _, graph_id);

            if redis_module::RedisModule_KeyType.unwrap_unchecked()(key)
                == redis_module::KeyType::Empty as i32
            {
                ErrorCtx_SetError(
                    c"Encountered an empty key when opened key %s".as_ptr(),
                    (*gc).graph_name,
                );

                redis_module::RedisModule_CloseKey.unwrap_unchecked()(key as *mut _);
                self.thread_safe_context_unlock();

                ErrorCtx_RaiseRuntimeException(null());
                return false;
            }

            if redis_module::RedisModule_ModuleTypeGetType.unwrap_unchecked()(key)
                != crate::GraphContextRedisModuleType
            {
                ErrorCtx_SetError(
                    c"Encountered a non-graph value type when opened key %s".as_ptr(),
                    (*gc).graph_name,
                );

                redis_module::RedisModule_CloseKey.unwrap_unchecked()(key as *mut _);
                self.thread_safe_context_unlock();

                ErrorCtx_RaiseRuntimeException(null());
                return false;
            }

            if gc != redis_module::RedisModule_ModuleTypeGetValue.unwrap_unchecked()(key) as *mut _
            {
                ErrorCtx_SetError(
                    c"Encountered different graph value when opened key %s".as_ptr(),
                    (*gc).graph_name,
                );

                redis_module::RedisModule_CloseKey.unwrap_unchecked()(key as *mut _);
                self.thread_safe_context_unlock();

                ErrorCtx_RaiseRuntimeException(null());
                return false;
            }

            self.internal_exec_ctx.key = key as *mut _;

            Graph_AcquireWriteLock(self.get_graph());
            self.internal_exec_ctx.locked_for_commit = true;

            true
        }
    }

    fn unlock_for_commit(&mut self) {
        if !self.internal_exec_ctx.locked_for_commit {
            return;
        }

        self.internal_exec_ctx.locked_for_commit = false;
        let graph = self.get_graph();

        unsafe {
            Graph_ReleaseLock(graph);

            redis_module::RedisModule_CloseKey.unwrap_unchecked()(
                self.internal_exec_ctx.key as *mut _,
            );

            self.thread_safe_context_unlock();
        }
    }

    // retrieve or instantiate new QueryCtx
    fn get_or_create_context() -> *mut QueryCtx {
        unsafe {
            let mut ctx: *mut QueryCtx = libc::pthread_getspecific(_tlsQueryCtxKey) as *mut _;
            if ctx.is_null() {
                ctx = Box::into_raw(Box::new(QueryCtx::new()));

                libc::pthread_setspecific(_tlsQueryCtxKey, ctx as *mut _);
            }

            ctx
        }
    }
}

impl Drop for QueryCtx {
    fn drop(&mut self) {
        if !self.undo_log.is_null() {
            unsafe {
                drop(Box::from_raw(self.undo_log));
            }
        }

        if !self.effects_buffer.is_null() {
            unsafe {
                EffectsBuffer_Free(self.effects_buffer);
            }
        }

        if !self.query_data.params.is_null() {
            unsafe { raxFreeWithCallback(self.query_data.params, QueryCtx_ParameterFreeCallback) }
            self.query_data.params = null_mut(); // In case someone still has the query_data itself
        }

        unsafe {
            libc::pthread_setspecific(_tlsQueryCtxKey, null_mut());
        }
    }
}

extern "C" {
    fn GraphContext_LogQuery(
        gc: *const GraphContext,
        received: u64,
        wait_duration: f64,
        execution_duration: f64,
        report_duration: f64,
        parameterized: bool,
        utilized_cache: bool,
        write: bool,
        timeout: bool,
        query: *const c_char,
    );
    fn Graph_AcquireWriteLock(graph: *mut Graph);
    fn Graph_ReleaseLock(graph: *mut Graph);
    fn Graph_ResetReservedNode(graph: *mut Graph);
    fn EffectsBuffer_New() -> *mut EffectsBuffer;
    fn EffectsBuffer_Free(effects_buffer: *mut EffectsBuffer);
    fn raxFreeWithCallback(
        params: *mut rax,
        callback: unsafe extern "C" fn(*mut c_void),
    );
    fn QueryCtx_ParameterFreeCallback(param: *mut c_void);
    fn simple_tic(tic: *mut f64);
    fn simple_toc(tic: *const f64) -> f64;
}
