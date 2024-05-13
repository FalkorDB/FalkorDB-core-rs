use super::Graph;
use crate::graph_entity::edge::Edge;
use crate::graph_entity::node::Node;
use crate::types::{LabelID, NodeID, RelationID};

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
}
