/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::ffi::{c_char, c_void};

use crate::graph::graph::Graph;

pub type NodeID = u64;
pub type EdgeID = u64;
pub type EntityID = u64;
pub type LabelID = i32;
pub type SchemaID = i32;
pub type RelationID = i32;
pub type AttributeID = i32;
pub type AttributeSet = *mut c_void;
pub type GraphContext = c_void;

pub type DataBlock = c_void;
pub type DataBlockIterator = c_void;

#[repr(C)]
#[allow(dead_code)]
#[derive(Copy, Clone, PartialEq)]
pub enum SchemaType {
    Node,
    Edge,
}

#[repr(C)]
#[allow(dead_code)]
#[derive(Copy, Clone)]
pub enum IndexFieldType {
    Unknown = 0x00,
    Fulltext = 0x01,
    Numeric = 0x02,
    Geo = 0x04,
    String = 0x08,
    Vector = 0x10,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Node {
    pub attributes: *mut AttributeSet,
    pub id: EntityID,
}

impl Node {
    pub fn set_attributes(
        &mut self,
        set: *mut AttributeSet,
    ) {
        unsafe {
            AttributeSet_Free(self.attributes);
            self.attributes.write(*set);
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct GraphEntity {
    pub attributes: *mut AttributeSet,
    pub id: EntityID,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Edge {
    pub attributes: *mut AttributeSet,
    pub id: EntityID,
    pub relationship: *const i8,
    pub relation_id: RelationID,
    pub src_id: NodeID,
    pub dest_id: NodeID,
}

impl Edge {
    pub fn set_attributes(
        &mut self,
        set: *mut AttributeSet,
    ) {
        unsafe {
            AttributeSet_Free(self.attributes);
            self.attributes.write(*set);
        }
    }
}

#[repr(C)]
#[allow(dead_code)]
#[allow(non_camel_case_types)]
pub enum ConfigOptionField {
    /// timeout value for queries
    TIMEOUT = 0,
    /// default timeout for read and write queries
    TIMEOUT_DEFAULT = 1,
    /// max timeout that can be enforced
    TIMEOUT_MAX = 2,
    /// number of entries in cache
    CACHE_SIZE = 3,
    /// delete graph asynchronously
    ASYNC_DELETE = 4,
    // max number of OpenMP threads to use
    OPENMP_NTHREAD = 5,
    /// number of threads in thread pool
    THREAD_POOL_SIZE = 6,
    /// max number of records in result-set
    RESULTSET_MAX_SIZE = 7,
    /// max number of elements in vkey
    VKEY_MAX_ENTITY_COUNT = 8,
    /// max number of queued queries
    MAX_QUEUED_QUERIES = 9,
    /// max mem(bytes) that query/thread can utilize at any given time       
    QUERY_MEM_CAPACITY = 10,
    /// number of pending changes before Delta_Matrix flushed
    DELTA_MAX_PENDING_CHANGES = 11,
    /// size of buffer to maintain as margin in matrices
    NODE_CREATION_BUFFER = 12,
    /// toggle on/off the GRAPH.INFO
    CMD_INFO = 13,
    /// the max number of info queries count
    CMD_INFO_MAX_QUERY_COUNT = 14,
    /// bolt protocol port
    EFFECTS_THRESHOLD = 15,
    /// replicate queries via effects
    BOLT_PORT = 16,
}

extern "C" {
    fn GraphContext_GetGraph(gc: *mut GraphContext) -> *mut Graph;
    fn GraphContext_RemoveSchema(
        gc: *mut GraphContext,
        schema_id: i32,
        t: SchemaType,
    );
    fn GraphContext_RemoveAttribute(
        gc: *mut GraphContext,
        id: AttributeID,
    );
    fn GraphContext_DeleteIndex(
        gc: *mut GraphContext,
        schema_type: SchemaType,
        label: *const c_char,
        field: *const c_char,
        t: IndexFieldType,
    ) -> i32;
    fn GraphContext_AddNodeToIndices(
        gc: *mut GraphContext,
        n: *mut Node,
    );
    fn GraphContext_AddEdgeToIndices(
        gc: *mut GraphContext,
        e: *mut Edge,
    );
    fn GraphContext_DeleteNodeFromIndices(
        gc: *mut GraphContext,
        n: *mut Node,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    fn GraphContext_DeleteEdgeFromIndices(
        gc: *mut GraphContext,
        e: *mut Edge,
    );
    pub fn AttributeSet_Free(set: *mut AttributeSet);
    pub fn Config_Option_get(
        field: ConfigOptionField,
        ...
    ) -> bool;
    #[allow(dead_code)]
    pub fn Config_Option_set(
        field: ConfigOptionField,
        val: *const c_char,
        err: *mut *mut c_char,
    ) -> bool;
    pub fn DataBlock_New(
        blockCap: u64,
        itemCap: u64,
        itemSize: u32,
        fp: unsafe extern "C" fn(*mut c_void),
    ) -> *mut DataBlock;
    pub fn DataBlock_ItemCap(dataBlock: *const DataBlock) -> u64;
    pub fn DataBlock_DeletedItemsCount(dataBlock: *const DataBlock) -> u32;
    pub fn DataBlock_ItemCount(dataBlock: *const DataBlock) -> u64;
    pub fn DataBlock_Accommodate(
        dataBlock: *mut DataBlock,
        k: i64,
    );
    pub fn DataBlock_GetReservedIdx(
        dataBlock: *const DataBlock,
        n: u64,
    ) -> u64;
    pub fn DataBlock_AllocateItem(
        dataBlock: *mut DataBlock,
        idx: *mut u64,
    ) -> *mut c_void;
    pub fn DataBlock_DeleteItem(
        dataBlock: *mut DataBlock,
        idx: u64,
    );
    pub fn DataBlock_ItemIsDeleted(item: *mut c_void) -> bool;
    pub fn DataBlock_GetItem(
        dataBlock: *const DataBlock,
        idx: u64,
    ) -> *mut c_void;
    pub fn DataBlock_Scan(dataBlock: *const DataBlock) -> *mut DataBlockIterator;
    pub fn DataBlock_Ensure(
        dataBlock: *const DataBlock,
        idx: u64,
    );
    pub fn DataBlock_MarkAsDeletedOutOfOrder(
        dataBlock: *const DataBlock,
        idx: u64,
    );
    pub fn DataBlock_AllocateItemOutOfOrder(
        dataBlock: *const DataBlock,
        idx: u64,
    ) -> *mut c_void;
    pub fn DataBlock_DeletedItems(dataBlock: *const DataBlock) -> *mut u64;
}

pub struct GraphContextAPI {
    pub context: *mut GraphContext,
}

impl GraphContextAPI {
    pub fn get_graph(&self) -> &mut Graph {
        unsafe { GraphContext_GetGraph(self.context).as_mut().unwrap() }
    }

    pub fn remove_schema(
        &self,
        schema_id: i32,
        t: SchemaType,
    ) {
        unsafe {
            GraphContext_RemoveSchema(self.context, schema_id, t);
        }
    }
    pub fn remove_attribute(
        &self,
        id: AttributeID,
    ) {
        unsafe {
            GraphContext_RemoveAttribute(self.context, id);
        }
    }
    pub fn delete_index(
        &self,
        schema_type: SchemaType,
        label: *const c_char,
        field: *const c_char,
        t: IndexFieldType,
    ) -> i32 {
        unsafe { GraphContext_DeleteIndex(self.context, schema_type, label, field, t) }
    }

    pub fn add_node_to_indices(
        &self,
        n: *mut Node,
    ) {
        unsafe {
            GraphContext_AddNodeToIndices(self.context, n);
        }
    }
    pub fn add_edge_to_indices(
        &self,
        e: *mut Edge,
    ) {
        unsafe {
            GraphContext_AddEdgeToIndices(self.context, e);
        }
    }
    pub fn delete_node_from_indices(
        &self,
        n: *mut Node,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            GraphContext_DeleteNodeFromIndices(self.context, n, lbls, lbl_count);
        }
    }
    pub fn delete_edge_from_indices(
        &self,
        e: *mut Edge,
    ) {
        unsafe {
            GraphContext_DeleteEdgeFromIndices(self.context, e);
        }
    }
    // pub fn AttributeSet_Free(set: *mut AttributeSet);
}
