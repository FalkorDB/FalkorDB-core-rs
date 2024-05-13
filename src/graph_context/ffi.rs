use super::GraphContext;
use crate::graph::Graph;
use crate::graph_entity::edge::Edge;
use crate::graph_entity::node::Node;
use crate::index::IndexFieldType;
use crate::schema::SchemaType;
use crate::types::{AttributeID, LabelID};
use std::ffi::c_char;

extern "C" {
    pub fn GraphContext_GetGraph(gc: *mut GraphContext) -> *mut Graph;
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
}
