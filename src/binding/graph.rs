use std::ffi::{c_char, c_void};

pub type NodeID = i64;
pub type EntityID = i64;
pub type LabelID = i32;
pub type SchemaID = i32;
pub type RelationID = i32;
pub type AttributeID = i32;
pub type AttributeSet = *mut c_void;

#[repr(C)]
pub struct Graph {
    // TODO
}

#[repr(C)]
pub struct GraphContext {
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

impl Node {
    pub fn set_attributes(&mut self, set: *mut AttributeSet) {
        unsafe{ 
            AttributeSet_Free(self.attributes);
            self.attributes.write(*set);
        }
    }
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
    pub fn set_attributes(&mut self, set: *mut AttributeSet) {
        unsafe{ 
            AttributeSet_Free(self.attributes);
            self.attributes.write(*set);
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
}

pub struct GraphAPI {
    pub graph: *mut Graph,
}

impl GraphAPI {
    pub fn create_node(
        &mut self,
        n: *mut Node,
        labels: *mut LabelID,
        label_count: u32,
    ) {
        unsafe {
            Graph_CreateNode(self.graph, n, labels, label_count);
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
            Graph_CreateEdge(self.graph, src, dest, r, e);
        }
    }
    pub fn delete_nodes(
        &mut self,
        nodes: *mut Node,
        count: u64,
    ) {
        unsafe {
            Graph_DeleteNodes(self.graph, nodes, count);
        }
    }
    pub fn delete_edges(
        &mut self,
        edges: *mut Edge,
        count: u64,
    ) {
        unsafe {
            Graph_DeleteEdges(self.graph, edges, count);
        }
    }
    pub fn label_node(
        &mut self,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            Graph_LabelNode(self.graph, id, lbls, lbl_count);
        }
    }
    pub fn remove_node_labels(
        &mut self,
        id: NodeID,
        lbls: *mut LabelID,
        lbl_count: u32,
    ) {
        unsafe {
            Graph_RemoveNodeLabels(self.graph, id, lbls, lbl_count);
        }
    }
    pub fn remove_label(
        &mut self,
        label_id: LabelID,
    ) {
        unsafe {
            Graph_RemoveLabel(self.graph, label_id);
        }
    }
    pub fn remove_relation(
        &mut self,
        relation_id: RelationID,
    ) {
        unsafe {
            Graph_RemoveRelation(self.graph, relation_id);
        }
    }
}

pub struct GraphContextAPI {
    pub context: *mut GraphContext,
}

impl GraphContextAPI {
    pub fn get_graph(&self) -> GraphAPI {
        unsafe {
            GraphAPI {
                graph: GraphContext_GetGraph(self.context),
            }
        }
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
