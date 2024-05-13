use crate::graph_entity::edge::Edge;
use crate::graph_entity::node::Node;
use crate::types::{LabelID, NodeID, RelationID};
use ffi::{
    Graph_CreateEdge, Graph_CreateNode, Graph_DeleteEdges, Graph_DeleteNodes, Graph_LabelNode,
    Graph_RemoveLabel, Graph_RemoveNodeLabels, Graph_RemoveRelation,
};

mod ffi;

#[repr(C)]
pub struct Graph {
    _unused: [u8; 0],
}

impl Graph {
    pub fn create_node(
        &mut self,
        n: *mut Node,
        labels: *mut LabelID,
        label_count: u32,
    ) {
        unsafe {
            Graph_CreateNode(self, n, labels, label_count);
        }
    }
    pub fn create_edge(
        &mut self,
        src: NodeID,
        dest: NodeID,
        r: RelationID,
        e: *mut Edge,
    ) {
        unsafe {
            Graph_CreateEdge(self, src, dest, r, e);
        }
    }
    pub fn delete_nodes(
        &mut self,
        nodes: *mut Node,
        count: u64,
    ) {
        unsafe {
            Graph_DeleteNodes(self, nodes, count);
        }
    }
    pub fn delete_edges(
        &mut self,
        edges: *mut Edge,
        count: u64,
    ) {
        unsafe {
            Graph_DeleteEdges(self, edges, count);
        }
    }
    pub fn label_node(
        &mut self,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            Graph_LabelNode(self, id, lbls, lbl_count);
        }
    }
    pub fn remove_node_labels(
        &mut self,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            Graph_RemoveNodeLabels(self, id, lbls, lbl_count);
        }
    }
    pub fn remove_label(
        &mut self,
        label_id: LabelID,
    ) {
        unsafe {
            Graph_RemoveLabel(self, label_id);
        }
    }
    pub fn remove_relation(
        &mut self,
        relation_id: RelationID,
    ) {
        unsafe {
            Graph_RemoveRelation(self, relation_id);
        }
    }
}
