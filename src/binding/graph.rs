use std::ffi::{c_char, c_void};

pub type NodeID = i64;
pub type EntityID = i64;
pub type LabelID = i32;
pub type SchemaID = i32;
pub type RelationID = i32;
pub type AttributeID = i32;
pub type GraphContext = c_void;
pub type AttributeSet = *mut c_void;

#[repr(C)]
pub struct Graph {
    // TODO
}

#[repr(C)]
#[derive(Copy, Clone, PartialEq)]
pub enum SchemaType {
    Node,
    Edge,
}

#[repr(C)]
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

extern "C" {
    pub fn Graph_CreateNode(
        g: *mut Graph,
        n: *mut Node,
        labels: *mut LabelID,
        label_count: u32,
    );
    pub fn Graph_CreateEdge(
        g: *mut Graph,
        src: NodeID,
        dest: NodeID,
        r: RelationID,
        e: *mut Edge,
    );
    pub fn Graph_DeleteNodes(
        g: *mut Graph,
        nodes: *mut Node,
        count: u64,
    );
    pub fn Graph_DeleteEdges(
        g: *mut Graph,
        edges: *mut Edge,
        count: u64,
    );
    pub fn Graph_LabelNode(
        g: *mut Graph,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    pub fn Graph_RemoveNodeLabels(
        g: *mut Graph,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    pub fn Graph_RemoveLabel(
        g: *mut Graph,
        label_id: LabelID,
    );
    pub fn Graph_RemoveRelation(
        g: *mut Graph,
        relation_id: RelationID,
    );
    pub fn GraphContext_RemoveSchema(
        gc: *mut GraphContext,
        schema_id: i32,
        t: SchemaType,
    );
    pub fn GraphContext_RemoveAttribute(
        gc: *mut GraphContext,
        id: AttributeID,
    );
    pub fn GraphContext_DeleteIndex(
        gc: *mut GraphContext,
        schema_type: SchemaType,
        label: *const c_char,
        field: *const c_char,
        t: IndexFieldType,
    ) -> i32;
    pub fn GraphContext_AddNodeToIndices(
        gc: *mut GraphContext,
        n: *mut Node,
    );
    pub fn GraphContext_AddEdgeToIndices(
        gc: *mut GraphContext,
        e: *mut Edge,
    );
    pub fn GraphContext_DeleteNodeFromIndices(
        gc: *mut GraphContext,
        n: *mut Node,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    pub fn GraphContext_DeleteEdgeFromIndices(
        gc: *mut GraphContext,
        e: *mut Edge,
    );
    pub fn AttributeSet_Free(set: *mut AttributeSet);
}
