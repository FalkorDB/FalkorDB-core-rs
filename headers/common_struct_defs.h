//
// Created by emily on 5/7/24.
//

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <bits/pthreadtypes.h>

typedef struct _UndoLog *UndoLog;
typedef struct _AST AST;
typedef struct _rax rax;
typedef struct _EffectsBuffer EffectsBuffer;
typedef struct _RedisModuleKey RedisModuleKey;
typedef struct _RedisModuleCtx RedisModuleCtx;
typedef struct _ResultSet ResultSet;
typedef struct _RedisModuleBlockedClient RedisModuleBlockedClient;
typedef struct _bolt_client_t bolt_client_t;
typedef struct _DataBlock DataBlock;
typedef struct _Schema Schema;
typedef struct _SlowLog SlowLog;
typedef struct _GraphEncodeContext GraphEncodeContext;
typedef struct _GraphDecodeContext GraphDecodeContext;
typedef struct _Delta_Matrix _Delta_Matrix;
typedef struct _Cache Cache;
typedef struct _RedisModuleString RedisModuleString;
typedef struct _QueriesLog *QueriesLog;

typedef _Delta_Matrix *Delta_Matrix;


typedef double simple_timer_t[2];
typedef uint32_t XXH32_hash_t;
typedef uint64_t GrB_Index;


#define GRAPH_DEFAULT_RELATION_TYPE_CAP 16  // default number of different relationship types a graph can hold before resizing.
#define GRAPH_DEFAULT_LABEL_CAP 16          // default number of different labels a graph can hold before resizing.
#define GRAPH_NO_LABEL -1                   // labels are numbered [0-N], -1 represents no label.
#define GRAPH_UNKNOWN_LABEL -2              // labels are numbered [0-N], -2 represents an unknown relation.
#define GRAPH_NO_RELATION -1                // relations are numbered [0-N], -1 represents no relation.
#define GRAPH_UNKNOWN_RELATION -2           // relations are numbered [0-N], -2 represents an unknown relation.

typedef enum {
    GRAPH_EDGE_DIR_INCOMING,
    GRAPH_EDGE_DIR_OUTGOING,
    GRAPH_EDGE_DIR_BOTH,
} GRAPH_EDGE_DIR;

typedef enum {
    SYNC_POLICY_UNKNOWN,
    SYNC_POLICY_FLUSH_RESIZE,
    SYNC_POLICY_RESIZE,
    SYNC_POLICY_NOP,
} MATRIX_POLICY;

// forward declaration of Graph struct
typedef struct Graph Graph;

// typedef for synchronization function pointer
typedef void (*SyncMatrixFunc)(const Graph *, Delta_Matrix, GrB_Index, GrB_Index);

typedef struct {
    Delta_Matrix R;     // relation matrix
    Delta_Matrix S;      // sources matrix
    Delta_Matrix T;      // targets matrix
} RelationMatrices;

struct Graph {
    int reserved_node_count;           // number of nodes not commited yet
    DataBlock *nodes;                  // graph nodes stored in blocks
    DataBlock *edges;                  // graph edges stored in blocks
    Delta_Matrix adjacency_matrix;     // adjacency matrix, holds all graph connections
    Delta_Matrix *labels;              // label matrices
    Delta_Matrix node_labels;          // mapping of all node IDs to all labels possessed by each node
    RelationMatrices *relations;       // relation matrices
    Delta_Matrix _zero_matrix;         // zero matrix
    pthread_rwlock_t _rwlock;          // read-write lock scoped to this specific graph
    bool _writelocked;                 // true if the read-write lock was acquired by a writer
    SyncMatrixFunc SynchronizeMatrix;  // function pointer to matrix synchronization routine
};

// GraphContext holds refrences to various elements of a graph object
// It is the value sitting behind a Redis graph key
//
// the graph context is versioned, the version value itself is meaningless
// it is used as a "signature" for the graph schema: (labels, relationship-types
// and attribute set) client libraries which cache the mapping between graph
// schema elements and their internal IDs (see COMPACT reply formatter)
// can use the graph version to understand if the schema was modified
// and take action accordingly

typedef struct {
    Graph *g;                              // container for all matrices and entity properties
    int ref_count;                         // number of active references
    rax *attributes;                       // from strings to attribute IDs
    pthread_rwlock_t _attribute_rwlock;    // read-write lock to protect access to the attribute maps
    char *graph_name;                      // string associated with graph
    char **string_mapping;                 // from attribute IDs to strings
    Schema **node_schemas;                 // array of schemas for each node label
    Schema **relation_schemas;             // array of schemas for each relation type
    unsigned short index_count;            // number of indicies
    SlowLog *slowlog;                      // slowlog associated with graph
    QueriesLog queries_log;                // log last x executed queries
    GraphEncodeContext *encoding_context;  // encode context of the graph
    GraphDecodeContext *decoding_context;  // decode context of the graph
    Cache *cache;                          // global cache of execution plans
    XXH32_hash_t version;                  // graph version
    RedisModuleString *telemetry_stream;   // telemetry stream name
} GraphContext;
