/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

 use std::{
    ffi::{c_char, c_void},
    ptr::null_mut,
};

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

pub type NodeID = i64;
pub type EntityID = i64;
pub type LabelID = i32;
pub type SchemaID = i32;
pub type RelationID = i32;
pub type AttributeID = i32;
pub type GraphContext = c_void;
pub type AttributeSet = *mut c_void;

enum UndoOp {
    CreateNodes(Vec<Node>),
    CreateEdges(Vec<Edge>),
    DeleteNodes(Vec<(NodeID, AttributeSet, Vec<LabelID>)>),
    DeleteEdges(Vec<(EntityID, NodeID, NodeID, RelationID, AttributeSet)>),
    UpdateNodes(Vec<(Node, AttributeSet)>),
    UpdateEdges(Vec<(Edge, AttributeSet)>),
    AddLabels(Vec<(Node, Vec<LabelID>)>),
    RemoveLabels(Vec<(Node, Vec<LabelID>)>),
    AddSchema(SchemaID, SchemaType),
    AddAttribute(AttributeID),
    CreateIndex(SchemaType, *const c_char, *const c_char, IndexFieldType),
}

/// cbindgen:ignore
pub struct _UndoLog {
    ops: Vec<UndoOp>,
}

impl _UndoLog {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn create_node(
        &mut self,
        node: Node,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::CreateNodes(vec![node]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::CreateNodes(nodes) = last_op {
            nodes.push(node);
        } else {
            self.ops.push(UndoOp::CreateNodes(vec![node]));
        }
    }

    pub fn create_edge(
        &mut self,
        edge: Edge,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::CreateEdges(vec![edge]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::CreateEdges(edges) = last_op {
            edges.push(edge);
        } else {
            self.ops.push(UndoOp::CreateEdges(vec![edge]));
        }
    }

    pub fn delete_node(
        &mut self,
        node_id: NodeID,
        set: AttributeSet,
        labels: Vec<LabelID>,
    ) {
        if self.ops.is_empty() {
            self.ops
                .push(UndoOp::DeleteNodes(vec![(node_id, set, labels)]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::DeleteNodes(vec) = last_op {
            vec.push((node_id, set, labels));
        } else {
            self.ops
                .push(UndoOp::DeleteNodes(vec![(node_id, set, labels)]));
        }
    }

    pub fn delete_edge(
        &mut self,
        edge_id: EntityID,
        src_id: NodeID,
        dest_id: NodeID,
        relation_id: RelationID,
        set: AttributeSet,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::DeleteEdges(vec![(
                edge_id,
                src_id,
                dest_id,
                relation_id,
                set,
            )]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::DeleteEdges(vec) = last_op {
            vec.push((edge_id, src_id, dest_id, relation_id, set));
        } else {
            self.ops.push(UndoOp::DeleteEdges(vec![(
                edge_id,
                src_id,
                dest_id,
                relation_id,
                set,
            )]));
        }
    }

    pub fn update_node(
        &mut self,
        node: Node,
        old_set: AttributeSet,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::UpdateNodes(vec![(node, old_set)]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::UpdateNodes(vec) = last_op {
            vec.push((node, old_set));
        } else {
            self.ops.push(UndoOp::UpdateNodes(vec![(node, old_set)]));
        }
    }

    pub fn update_edge(
        &mut self,
        edge: Edge,
        old_set: AttributeSet,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::UpdateEdges(vec![(edge, old_set)]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::UpdateEdges(vec) = last_op {
            vec.push((edge, old_set));
        } else {
            self.ops.push(UndoOp::UpdateEdges(vec![(edge, old_set)]));
        }
    }

    pub fn add_labels(
        &mut self,
        node: Node,
        labels: Vec<LabelID>,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::AddLabels(vec![(node, labels)]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::AddLabels(vec) = last_op {
            vec.push((node, labels));
        } else {
            self.ops.push(UndoOp::AddLabels(vec![(node, labels)]));
        }
    }

    pub fn remove_labels(
        &mut self,
        node: Node,
        labels: Vec<LabelID>,
    ) {
        if self.ops.is_empty() {
            self.ops.push(UndoOp::RemoveLabels(vec![(node, labels)]));
            return;
        }
        let last_op = self.ops.last_mut().unwrap();
        if let UndoOp::RemoveLabels(vec) = last_op {
            vec.push((node, labels));
        } else {
            self.ops.push(UndoOp::RemoveLabels(vec![(node, labels)]));
        }
    }

    pub fn add_schema(
        &mut self,
        schema_id: SchemaID,
        schema_type: SchemaType,
    ) {
        self.ops.push(UndoOp::AddSchema(schema_id, schema_type));
    }

    pub fn add_attribute(
        &mut self,
        attribute_id: AttributeID,
    ) {
        self.ops.push(UndoOp::AddAttribute(attribute_id));
    }

    pub fn create_index(
        &mut self,
        schema_type: SchemaType,
        label: *const c_char,
        field: *const c_char,
        index_field_type: IndexFieldType,
    ) {
        self.ops.push(UndoOp::CreateIndex(
            schema_type,
            label,
            field,
            index_field_type,
        ));
    }

    pub unsafe fn rollback(
        &mut self,
        gc: *mut GraphContext,
        g: *mut Graph,
    ) {
        while let Some(op) = self.ops.pop() {
            match op {
                UndoOp::CreateNodes(mut nodes) => {
                    for node in nodes.iter_mut().rev() {
                        GraphContext_DeleteNodeFromIndices(gc, node);
                    }
                    Graph_DeleteNodes(g, nodes.as_mut_ptr(), nodes.len() as u64);
                }
                UndoOp::CreateEdges(mut edges) => {
                    for edge in edges.iter_mut().rev() {
                        GraphContext_DeleteEdgeFromIndices(gc, edge);
                    }
                    Graph_DeleteEdges(g, edges.as_mut_ptr(), edges.len() as u64);
                }
                UndoOp::DeleteNodes(mut vec) => {
                    for (node_id, set, labels) in vec.iter_mut().rev() {
                        let mut node = Node {
                            attributes: null_mut(),
                            id: *node_id,
                        };
                        Graph_CreateNode(g, &mut node, labels.as_mut_ptr(), labels.len() as u32);
                        node.attributes.write(*set);
                        GraphContext_AddNodeToIndices(gc, &mut node);
                    }
                }
                UndoOp::DeleteEdges(mut vec) => {
                    for (edge_id, src_id, dest_id, relation_id, set) in vec.iter_mut().rev() {
                        let mut edge = Edge {
                            attributes: null_mut(),
                            id: *edge_id,
                            relationship: null_mut(),
                            relation_id: *relation_id,
                            src_id: *src_id,
                            dest_id: *dest_id,
                        };
                        Graph_CreateEdge(g, edge.src_id, edge.dest_id, edge.relation_id, &mut edge);
                        edge.attributes.write(*set);
                        GraphContext_AddEdgeToIndices(gc, &mut edge);
                    }
                }
                UndoOp::UpdateNodes(mut vec) => {
                    for (node, old_set) in vec.iter_mut().rev() {
                        AttributeSet_Free(node.attributes);
                        node.attributes.write(*old_set);
                        GraphContext_AddNodeToIndices(gc, node);
                    }
                }
                UndoOp::UpdateEdges(mut vec) => {
                    for (edge, old_set) in vec.iter_mut().rev() {
                        AttributeSet_Free(edge.attributes);
                        edge.attributes.write(*old_set);
                        GraphContext_AddEdgeToIndices(gc, edge);
                    }
                }
                UndoOp::AddLabels(mut vec) => {
                    for (node, labels) in vec.iter_mut().rev() {
                        Graph_RemoveNodeLabels(
                            g,
                            node.id,
                            labels.as_mut_ptr(),
                            labels.len() as u32,
                        );
                        GraphContext_DeleteNodeFromIndicesByLabels(
                            gc,
                            node,
                            labels.as_mut_ptr(),
                            labels.len() as u32,
                        );
                    }
                }
                UndoOp::RemoveLabels(mut vec) => {
                    for (node, labels) in vec.iter_mut().rev() {
                        Graph_LabelNode(g, node.id, labels.as_mut_ptr(), labels.len() as u32);
                        GraphContext_AddNodeToIndices(gc, node);
                    }
                }
                UndoOp::AddSchema(schema_id, schema_type) => {
                    GraphContext_RemoveSchema(gc, schema_id, schema_type);
                    if schema_type == SchemaType::Node {
                        Graph_RemoveLabel(g, schema_id);
                    } else {
                        Graph_RemoveRelation(g, schema_id);
                    }
                }
                UndoOp::AddAttribute(attribute_id) => {
                    GraphContext_RemoveAttribute(gc, attribute_id);
                }
                UndoOp::CreateIndex(schema_type, label, field, index_field_type) => {
                    GraphContext_DeleteIndex(gc, schema_type, label, field, index_field_type);
                }
            }
        }
    }
}

extern "C" {
    fn Graph_CreateNode(
        g: *mut Graph,
        n: *mut Node,
        labels: *mut LabelID,
        label_count: u32,
    );
    fn Graph_CreateEdge(
        g: *mut Graph,
        src: NodeID,
        dest: NodeID,
        r: RelationID,
        e: *mut Edge,
    );
    fn Graph_DeleteNodes(
        g: *mut Graph,
        nodes: *mut Node,
        count: u64,
    );
    fn Graph_DeleteEdges(
        g: *mut Graph,
        edges: *mut Edge,
        count: u64,
    );
    fn Graph_LabelNode(
        g: *mut Graph,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    fn Graph_RemoveNodeLabels(
        g: *mut Graph,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    fn Graph_RemoveLabel(
        g: *mut Graph,
        label_id: LabelID,
    );
    fn Graph_RemoveRelation(
        g: *mut Graph,
        relation_id: RelationID,
    );
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
    );
    fn GraphContext_DeleteNodeFromIndicesByLabels(
        gc: *mut GraphContext,
        n: *mut Node,
        lbls: *mut LabelID,
        lbl_count: u32,
    );
    fn GraphContext_DeleteEdgeFromIndices(
        gc: *mut GraphContext,
        e: *mut Edge,
    );
    fn AttributeSet_Free(set: *mut AttributeSet);
}
