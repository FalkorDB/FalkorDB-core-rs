//
// Created by emily on 5/7/24.
//

#pragma once

#include <stdint.h>
#include <stdbool.h>

#include "common_struct_defs.h"

// holds the execution type flags: certain traits of query regarding its
// execution
typedef enum QueryExecutionTypeFlag {
    // indicates that this query is a read-only query
    QueryExecutionTypeFlag_READ = 0,
    // indicates that this query is a write query
    QueryExecutionTypeFlag_WRITE = 1 << 0,
    // whether or not we want to profile the query
    QueryExecutionTypeFlag_PROFILE = 1 << 1,
} QueryExecutionTypeFlag;

// holds the query execution status
typedef enum QueryExecutionStatus {
    QueryExecutionStatus_SUCCESS = 0,
    QueryExecutionStatus_FAILURE,
    QueryExecutionStatus_TIMEDOUT,
} QueryExecutionStatus;

// stages a query may be in
typedef enum QueryStage {
    QueryStage_WAITING = 0,
    QueryStage_EXECUTING = 1,
    QueryStage_REPORTING = 2,
    QueryStage_FINISHED = 3,
} QueryStage;

typedef struct {
    AST *ast;                     // the scoped AST associated with this query
    rax *params;                  // query parameters
    const char *query;            // query string
    const char *query_no_params;  // query string without parameters part
} QueryCtx_QueryData;

typedef struct {
    RedisModuleKey *key;     // graph open key, for later extraction and closing
    ResultSet *result_set;   // execution result set
    bool locked_for_commit;  // indicates if QueryCtx_LockForCommit been called
} QueryCtx_InternalExecCtx;

typedef struct {
    RedisModuleCtx *redis_ctx;     // the Redis module context
    RedisModuleBlockedClient *bc;  // blocked client
    bolt_client_t *bolt_client;    // bolt client
    const char *command_name;      // command name
} QueryCtx_GlobalExecCtx;

// query statistics
typedef struct {
    simple_timer_t timer;  // stage timer
    uint64_t received_ts;  // query received timestamp
    double durations[3];   // stage durations
    bool parameterized;    // uses parameters
    bool utilized_cache;   // utilized cache
} QueryStats;

typedef struct QueryCtx {
    QueryStats stats;                            // query statistics
    GraphContext *gc;                            // GraphContext associated with this query's graph
    UndoLog undo_log;                            // undo-log in case rollback is needed
    QueryStage stage;                            // query execution stage
    QueryExecutionStatus status;                 // query execution status
    QueryExecutionTypeFlag flags;                // execution flags
    EffectsBuffer *effects_buffer;               // effects-buffer for replication, used when write query succeed and replication is needed
    QueryCtx_QueryData query_data;               // data related to the query syntax
    QueryCtx_GlobalExecCtx global_exec_ctx;      // data related to global redis execution
    QueryCtx_InternalExecCtx internal_exec_ctx;  // data related to internal query execution
} QueryCtx;