mod ffi;

use crate::graph::Graph;
use crate::graph_entity::edge::Edge;
use crate::graph_entity::node::Node;
use crate::index::IndexFieldType;
use crate::schema::SchemaType;
use crate::types::{AttributeID, LabelID};
use ffi::{
    GraphContext_AddEdgeToIndices, GraphContext_AddNodeToIndices,
    GraphContext_DeleteEdgeFromIndices, GraphContext_DeleteIndex,
    GraphContext_DeleteNodeFromIndices, GraphContext_GetGraph, GraphContext_RemoveAttribute,
    GraphContext_RemoveSchema,
};
use std::ffi::c_char;

#[repr(C)]
pub struct GraphContext {
    _unused: [u8; 0],
}

impl GraphContext {
    pub fn get_graph(&mut self) -> *mut Graph {
        unsafe { &mut (*GraphContext_GetGraph(self)) }
    }

    pub fn remove_schema(
        &mut self,
        schema_id: i32,
        t: SchemaType,
    ) {
        unsafe {
            GraphContext_RemoveSchema(self, schema_id, t);
        }
    }
    pub fn remove_attribute(
        &mut self,
        id: AttributeID,
    ) {
        unsafe {
            GraphContext_RemoveAttribute(self, id);
        }
    }
    pub fn delete_index(
        &mut self,
        schema_type: SchemaType,
        label: *const c_char,
        field: *const c_char,
        t: IndexFieldType,
    ) -> i32 {
        unsafe { GraphContext_DeleteIndex(self, schema_type, label, field, t) }
    }

    pub fn add_node_to_indices(
        &mut self,
        n: *mut Node,
    ) {
        unsafe {
            GraphContext_AddNodeToIndices(self, n);
        }
    }
    pub fn add_edge_to_indices(
        &mut self,
        e: *mut Edge,
    ) {
        unsafe {
            GraphContext_AddEdgeToIndices(self, e);
        }
    }
    pub fn delete_node_from_indices(
        &mut self,
        n: *mut Node,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            GraphContext_DeleteNodeFromIndices(self, n, lbls, lbl_count);
        }
    }
    pub fn delete_edge_from_indices(
        &mut self,
        e: *mut Edge,
    ) {
        unsafe {
            GraphContext_DeleteEdgeFromIndices(self, e);
        }
    }
}
