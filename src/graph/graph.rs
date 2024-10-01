/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{
    ffi::c_void,
    mem::{size_of, transmute_copy, MaybeUninit},
    ptr::null_mut,
};

use crate::{
    binding::{
        crwlock::CRWLock,
        graph::{
            AttributeSet, AttributeSet_Free, DataBlock, DataBlockIterator, DataBlock_Accommodate,
            DataBlock_AllocateItem, DataBlock_AllocateItemOutOfOrder, DataBlock_DeleteItem,
            DataBlock_DeletedItems, DataBlock_DeletedItemsCount, DataBlock_Ensure,
            DataBlock_GetItem, DataBlock_GetReservedIdx, DataBlock_ItemCap, DataBlock_ItemCount,
            DataBlock_MarkAsDeletedOutOfOrder, DataBlock_New, DataBlock_Scan, Edge, EdgeID,
            LabelID, Node, NodeID, RelationID,
        },
    },
    graph::matrix::GraphBLAS::{
        GrB_ALL, GrB_Info, GrB_Vector_free, GrB_Vector_new, GrB_transpose, GxB_Row_subassign,
        GxB_Vector_diag,
    },
    grb_check,
};

use super::matrix::{
    delta_matrix::DeltaMatrix, delta_matrix_iter::DeltaMatrixIter, sparse_matrix::SparseMatrix,
    tensor::Tensor, GraphBLAS::GrB_BOOL,
};

const INVALID_ENTITY_ID: NodeID = u64::MAX;

#[repr(C)]
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub enum MatrixPolicy {
    FlushResize,
    Resize,
    Nop,
}

#[repr(C)]
#[derive(PartialEq, Eq)]
pub enum GraphEdgeDir {
    Incoming,
    Outgoing,
    Both,
}

struct GraphStatistics {
    node_count: Vec<u64>,
    edge_count: Vec<u64>,
}
impl GraphStatistics {
    fn introduce_label(&mut self) {
        self.node_count.push(0);
    }

    fn increment_node_count(
        &mut self,
        l: LabelID,
        arg: u64,
    ) {
        self.node_count[l as usize] += arg;
    }

    fn decrement_node_count(
        &mut self,
        l: LabelID,
        arg: u64,
    ) {
        self.node_count[l as usize] -= arg;
    }

    fn introduce_relationship(&mut self) {
        self.edge_count.push(0);
    }

    fn increment_edge_count(
        &mut self,
        r: RelationID,
        arg: u64,
    ) {
        self.edge_count[r as usize] += arg;
    }

    fn decrement_edge_count(
        &mut self,
        r: RelationID,
        arg: u64,
    ) {
        self.edge_count[r as usize] -= arg;
    }
}

pub struct Graph {
    reserved_node_count: i32,
    nodes: *mut DataBlock,
    edges: *mut DataBlock,
    adjacency_matrix: DeltaMatrix,
    labels: Vec<DeltaMatrix>,
    node_labels: DeltaMatrix,
    relations: Vec<Tensor>,
    zero_matrix: DeltaMatrix,
    rwlock: CRWLock,
    writelocked: bool,
    matrix_policy: MatrixPolicy,
    stats: GraphStatistics,
}

impl Graph {
    pub fn new(
        node_cap: u64,
        edge_cap: u64,
    ) -> Self {
        let nodes = unsafe {
            DataBlock_New(
                node_cap,
                node_cap,
                size_of::<AttributeSet>() as u32,
                transmute_copy(&(AttributeSet_Free as *const c_void)),
            )
        };
        let edges = unsafe {
            DataBlock_New(
                edge_cap,
                edge_cap,
                size_of::<AttributeSet>() as u32,
                transmute_copy(&(AttributeSet_Free as *const c_void)),
            )
        };
        Self {
            reserved_node_count: 0,
            nodes: nodes,
            edges: edges,
            adjacency_matrix: DeltaMatrix::new(unsafe { GrB_BOOL }, node_cap, node_cap, true),
            labels: Vec::new(),
            node_labels: DeltaMatrix::new(unsafe { GrB_BOOL }, node_cap, node_cap, false),
            relations: Vec::new(),
            zero_matrix: DeltaMatrix::new(unsafe { GrB_BOOL }, node_cap, node_cap, false),
            rwlock: CRWLock::new(),
            writelocked: false,
            matrix_policy: MatrixPolicy::FlushResize,
            stats: GraphStatistics {
                node_count: Vec::new(),
                edge_count: Vec::new(),
            },
        }
    }

    pub fn acquire_read_lock(&mut self) {
        self.rwlock.acquire_read();
    }

    pub fn acquire_write_lock(&mut self) {
        debug_assert!(!self.writelocked);
        self.rwlock.acquire_write();
        self.writelocked = true;
    }

    pub fn release_lock(&mut self) {
        self.writelocked = false;
        self.rwlock.release();
    }

    pub fn apply_all_pending(
        &mut self,
        force_flush: bool,
    ) {
        let policy = self.set_matrix_policy(MatrixPolicy::FlushResize);

        self.get_adjacency_matrix(false).wait(force_flush);
        self.get_node_label_matrix().wait(force_flush);
        self.get_zero_matrix().wait(force_flush);

        for label in 0..self.labels.len() {
            self.get_label_matrix(label as LabelID).wait(force_flush);
        }

        for relation in 0..self.relations.len() {
            self.get_relation_matrix(relation as RelationID, false)
                .wait(force_flush);
        }

        self.set_matrix_policy(policy);
    }

    pub fn get_matrix_policy(&self) -> MatrixPolicy {
        self.matrix_policy
    }

    pub fn set_matrix_policy(
        &mut self,
        policy: MatrixPolicy,
    ) -> MatrixPolicy {
        let old_policy = self.matrix_policy;
        self.matrix_policy = policy;
        old_policy
    }

    pub fn pending(&self) -> bool {
        self.adjacency_matrix.pending()
            || self.node_labels.pending()
            || self.zero_matrix.pending()
            || self.labels.iter().any(|m| m.pending())
            || self.relations.iter().any(|m| m.m.pending())
    }

    pub fn add_label(&mut self) -> i32 {
        let n = self.required_matrix_dim();
        self.labels
            .push(DeltaMatrix::new(unsafe { GrB_BOOL }, n, n, false));
        self.stats.introduce_label();
        self.label_type_count() - 1
    }

    pub fn remove_label(
        &mut self,
        label_id: LabelID,
    ) {
        debug_assert!(label_id == self.labels.len() as i32 - 1);
        debug_assert!(self.labels[label_id as usize].nvals() == 0);
        self.labels.remove(label_id as usize);
    }

    pub fn label_node(
        &mut self,
        id: NodeID,
        lbls: &[LabelID],
    ) {
        if lbls.is_empty() {
            return;
        }
        let nl = self.get_node_label_matrix();
        for l in lbls {
            nl.set_element_bool(id, *l as u64);
        }
        for l in lbls {
            self.get_label_matrix(*l).set_element_bool(id, id);
            self.stats.increment_node_count(*l, 1);
        }
    }

    pub fn remove_node_labels(
        &mut self,
        id: NodeID,
        lbls: &[LabelID],
    ) {
        let nl = self.get_node_label_matrix();
        for l in lbls {
            nl.remove_element(id, *l as u64);
        }
        for l in lbls {
            self.get_label_matrix(*l).remove_element(id, id);
            self.stats.decrement_node_count(*l, 1);
        }
    }

    pub fn is_node_labeled(
        &mut self,
        id: NodeID,
        l: LabelID,
    ) -> bool {
        self.get_node_label_matrix()
            .extract_element_bool(id, l as u64)
            .is_some()
    }

    pub fn add_relation_type(&mut self) -> RelationID {
        let n = self.required_matrix_dim();
        let r = Tensor::new(n, n);
        self.relations.push(r);
        self.stats.introduce_relationship();
        self.relation_type_count() - 1
    }

    pub fn remove_relation(
        &mut self,
        relation_id: RelationID,
    ) {
        debug_assert!(relation_id == self.relations.len() as i32 - 1);
        debug_assert!(self.relations[relation_id as usize].m.nvals() == 0);
        self.relations.remove(relation_id as usize);
    }

    pub fn allocate_nodes(
        &self,
        n: usize,
    ) {
        unsafe { DataBlock_Accommodate(self.nodes, n as i64) };
    }

    pub fn allocate_edges(
        &self,
        n: usize,
    ) {
        unsafe { DataBlock_Accommodate(self.edges, n as i64) };
    }

    pub fn reset_reserved_node(&mut self) {
        self.reserved_node_count = 0;
    }

    pub fn reserve_node(&mut self) -> Node {
        let id = unsafe { DataBlock_GetReservedIdx(self.nodes, self.reserved_node_count as u64) };
        self.reserved_node_count += 1;
        Node {
            id,
            attributes: null_mut(),
        }
    }

    pub fn create_node(
        &mut self,
        n: &mut Node,
        labels: &[LabelID],
    ) {
        let id = n.id;
        n.attributes = unsafe { DataBlock_AllocateItem(self.nodes, &mut n.id) } as _;
        unsafe { n.attributes.write_unaligned(null_mut()) };

        if id != INVALID_ENTITY_ID {
            debug_assert!(id == n.id);
            self.reserved_node_count -= 1;
            debug_assert!(self.reserved_node_count >= 0);
        }

        self.label_node(n.id, labels);
    }

    pub fn create_edge(
        &mut self,
        src: NodeID,
        dest: NodeID,
        r: RelationID,
        e: &mut Edge,
    ) {
        debug_assert!(self.get_node(src).is_some());
        debug_assert!(self.get_node(dest).is_some());
        debug_assert!(r >= 0 && r < self.relations.len() as i32);

        e.src_id = src;
        e.dest_id = dest;
        e.relation_id = r;
        e.attributes = unsafe { DataBlock_AllocateItem(self.edges, &mut e.id) } as _;

        self.form_connection(src, dest, r, e);
    }

    fn form_connection(
        &mut self,
        src: u64,
        dest: u64,
        r: i32,
        e: &mut Edge,
    ) {
        self.get_relation_matrix(r, false);

        let adj = self.get_adjacency_matrix(false);

        adj.set_element_bool(src, dest);

        self.relations[r as usize].set_element(src, dest, e.id);
        self.stats.increment_edge_count(r, 1);
    }

    pub fn create_edges(
        &mut self,
        r: RelationID,
        edges: &mut [*mut Edge],
    ) {
        debug_assert!(r >= 0 && r < self.relations.len() as i32);

        edges.sort_by(|e1, e2| unsafe {
            if (**e1).src_id == (**e2).src_id {
                (**e1).dest_id.partial_cmp(&(**e2).dest_id).unwrap()
            } else {
                (**e1).src_id.partial_cmp(&(**e2).src_id).unwrap()
            }
        });

        debug_assert!(edges
            .iter()
            .all(|e| self.get_node(unsafe { (**e).src_id }).is_some()
                && self.get_node(unsafe { (**e).dest_id }).is_some()));

        for edge in edges.iter() {
            let e = unsafe { (*edge).as_mut().unwrap() };
            e.relation_id = r;
            e.attributes = unsafe { DataBlock_AllocateItem(self.edges, &mut e.id) } as _;
        }

        let adj = self.get_adjacency_matrix(false);
        for edge in edges.iter() {
            let src = unsafe { (**edge).src_id };
            let dest = unsafe { (**edge).dest_id };
            adj.set_element_bool(src, dest);
        }

        self.get_relation_matrix(r, false);

        self.relations[r as usize].set_elements(edges);
        self.stats.increment_edge_count(r, edges.len() as u64);
    }

    pub fn delete_nodes(
        &mut self,
        nodes: &[Node],
    ) {
        let policy = self.set_matrix_policy(MatrixPolicy::Nop);

        debug_assert!({
            let mut v = Vec::new();
            nodes.iter().all(|n| {
                self.get_node_edges(n, GraphEdgeDir::Both, -1, &mut v);
                v.is_empty()
            })
        });

        let mut elems = SparseMatrix::new(
            unsafe { GrB_BOOL },
            self.required_matrix_dim(),
            self.required_matrix_dim(),
        );

        for node in nodes {
            let mut it = DeltaMatrixIter::new_range(&self.node_labels, node.id, node.id);

            while let Ok(Some((_, j))) = it.next_bool() {
                elems.set_element_bool(true, node.id, j);
                self.labels[j as usize].remove_element(node.id, node.id);
                self.stats.decrement_node_count(j as LabelID, 1);
            }
            unsafe { DataBlock_DeleteItem(self.nodes, node.id) };
        }

        self.node_labels.remove_elements(&elems);

        self.set_matrix_policy(policy);
    }

    pub fn delete_edges(
        &mut self,
        edges: &mut [Edge],
    ) {
        let policy = self.set_matrix_policy(MatrixPolicy::Nop);

        for e in edges.iter() {
            unsafe { DataBlock_DeleteItem(self.edges, e.id) };
        }

        edges.sort_by(|e1, e2| {
            if e1.relation_id != e2.relation_id {
                e1.relation_id.partial_cmp(&e2.relation_id).unwrap()
            } else if e1.src_id != e2.src_id {
                e1.src_id.partial_cmp(&e2.src_id).unwrap()
            } else {
                e1.dest_id.partial_cmp(&e2.dest_id).unwrap()
            }
        });

        let mut i = 0;
        while i < edges.len() {
            let r = edges[i].relation_id;

            let mut j = i;
            while j < edges.len() && edges[j].relation_id == r {
                j += 1;
            }

            let flat_deletion = !self.relationship_contains_multi_edge(r);
            let d = j - i;
            self.stats.decrement_edge_count(r, d as u64);

            if flat_deletion {
                self.relations[r as usize].remove_elements_flat(&edges[i..j]);
                for edge in &edges[i..j] {
                    self.clear_adjacency_matrix_element(
                        edge.relation_id,
                        edge.src_id,
                        edge.dest_id,
                    );
                }
            } else {
                let cleard_entries = self.relations[r as usize].remove_elements(&edges[i..j]);
                for k in cleard_entries {
                    self.clear_adjacency_matrix_element(
                        edges[i + k].relation_id,
                        edges[i + k].src_id,
                        edges[i + k].dest_id,
                    );
                }
            }

            i = j;
        }

        self.set_matrix_policy(policy);
    }

    pub fn required_matrix_dim(&self) -> u64 {
        unsafe { DataBlock_ItemCap(self.nodes) }
    }

    pub fn scan_nodes(&self) -> *mut DataBlockIterator {
        unsafe { DataBlock_Scan(self.nodes) }
    }

    pub fn scan_edges(&self) -> *mut DataBlockIterator {
        unsafe { DataBlock_Scan(self.edges) }
    }

    pub fn node_count(&self) -> u64 {
        unsafe { DataBlock_ItemCount(self.nodes) }
    }

    pub fn deleted_node_count(&self) -> u64 {
        unsafe { DataBlock_DeletedItemsCount(self.nodes) as u64 }
    }

    pub fn uncompacted_node_count(&self) -> u64 {
        self.node_count() + self.deleted_node_count()
    }

    pub fn labeled_node_count(
        &self,
        label: i32,
    ) -> u64 {
        if label < 0 {
            return 0;
        }
        self.stats.node_count[label as usize]
    }

    pub fn edge_count(&self) -> usize {
        unsafe { DataBlock_ItemCount(self.edges) as usize }
    }

    pub fn relation_edge_count(
        &self,
        relation_idx: i32,
    ) -> u64 {
        if relation_idx < 0 {
            return 0;
        }
        self.stats.edge_count[relation_idx as usize]
    }

    pub fn deleted_edge_count(&self) -> u32 {
        unsafe { DataBlock_DeletedItemsCount(self.edges) }
    }

    pub fn relation_type_count(&self) -> i32 {
        self.relations.len() as i32
    }

    pub fn label_type_count(&self) -> i32 {
        self.labels.len() as i32
    }

    pub fn relationship_contains_multi_edge(
        &mut self,
        r: RelationID,
    ) -> bool {
        let a = self.relations[r as usize].m.nvals();
        let b = self.relation_edge_count(r);
        a != b
    }

    pub fn get_node(
        &self,
        id: NodeID,
    ) -> Option<Node> {
        let set = unsafe { DataBlock_GetItem(self.nodes, id) } as *mut AttributeSet;
        if set.is_null() {
            return None;
        }
        Some(Node {
            id,
            attributes: set,
        })
    }

    pub fn get_edge(
        &self,
        id: EdgeID,
        e: &mut Edge,
    ) -> bool {
        e.id = id;
        e.attributes = unsafe { DataBlock_GetItem(self.edges, id) } as _;
        !e.attributes.is_null()
    }

    pub fn get_edges_connecting_nodes(
        &mut self,
        src_id: NodeID,
        dest_id: NodeID,
        r: RelationID,
    ) -> Vec<Edge> {
        let mut edges = Vec::new();

        if r == -1 {
            let type_count = self.relation_type_count();
            for i in 0..type_count {
                self._get_edges_connecting_nodes(i, src_id, dest_id, &mut edges);
            }
        } else {
            self._get_edges_connecting_nodes(r, src_id, dest_id, &mut edges);
        }

        edges
    }

    fn _get_edges_connecting_nodes(
        &mut self,
        r: RelationID,
        src_id: u64,
        dest_id: u64,
        edges: &mut Vec<Edge>,
    ) {
        if r == -2 {
            return;
        }

        debug_assert!(r >= 0 && r < self.relations.len() as i32);

        self.get_relation_matrix(r, false);

        let mut e = Edge {
            src_id,
            dest_id,
            id: 0,
            attributes: null_mut(),
            relation_id: r,
            relationship: null_mut(),
        };

        let it = self.relations[r as usize].iter(src_id, dest_id);

        for (_, _, edge_id) in it {
            self.get_edge(edge_id, &mut e);
            edges.push(e);
        }
    }

    pub fn _get_outgoing_node_edges(
        &mut self,
        n: &Node,
        edge_type: i32,
        edges: &mut Vec<Edge>,
    ) {
        self.get_relation_matrix(edge_type, false);

        let it = self.relations[edge_type as usize].iter_range(n.id, n.id, false);
        for (src, dest, edge_id) in it {
            let e = Edge {
                src_id: src,
                dest_id: dest,
                id: edge_id,
                attributes: unsafe { DataBlock_GetItem(self.edges, edge_id) } as _,
                relation_id: edge_type,
                relationship: null_mut(),
            };
            debug_assert!(!e.attributes.is_null());
            edges.push(e);
        }
    }

    pub fn _get_incoming_node_edges(
        &mut self,
        n: &Node,
        edge_type: i32,
        edges: &mut Vec<Edge>,
    ) {
        self.get_relation_matrix(edge_type, false);

        let it = self.relations[edge_type as usize].iter_range(n.id, n.id, true);
        for (src, dest, edge_id) in it {
            let e = Edge {
                src_id: src,
                dest_id: dest,
                id: edge_id,
                attributes: unsafe { DataBlock_GetItem(self.edges, edge_id) } as _,
                relation_id: edge_type,
                relationship: null_mut(),
            };
            debug_assert!(!e.attributes.is_null());
            edges.push(e);
        }
    }

    pub fn get_node_edges(
        &mut self,
        n: &Node,
        dir: GraphEdgeDir,
        edge_type: RelationID,
        edges: &mut Vec<Edge>,
    ) {
        debug_assert!(edge_type >= -1 && edge_type < self.relations.len() as i32);

        if dir == GraphEdgeDir::Outgoing || dir == GraphEdgeDir::Both {
            if edge_type == -1 {
                for i in 0..self.relation_type_count() {
                    self._get_outgoing_node_edges(n, i, edges);
                }
            } else {
                self._get_outgoing_node_edges(n, edge_type, edges);
            }
        }

        if dir == GraphEdgeDir::Incoming || dir == GraphEdgeDir::Both {
            if edge_type == -1 {
                for i in 0..self.relation_type_count() {
                    self._get_incoming_node_edges(n, i, edges);
                }
            } else {
                self._get_incoming_node_edges(n, edge_type, edges);
            }
        }
    }

    pub fn get_node_degree(
        &mut self,
        n: &Node,
        dir: GraphEdgeDir,
        edge_type: RelationID,
    ) -> u64 {
        let mut count = 0u64;

        for r in if edge_type == -1 {
            0..self.relation_type_count()
        } else {
            edge_type..edge_type + 1
        } {
            self.get_relation_matrix(r, false);

            if dir == GraphEdgeDir::Outgoing || dir == GraphEdgeDir::Both {
                count += self.relations[r as usize].row_degree(n.id);
            }

            if dir == GraphEdgeDir::Incoming || dir == GraphEdgeDir::Both {
                count += self.relations[r as usize].col_degree(n.id);
            }
        }

        count
    }

    pub fn get_node_labels(
        &mut self,
        n: &Node,
        labels: &mut [i32],
    ) -> u32 {
        let m = self.get_node_label_matrix();

        let mut it = DeltaMatrixIter::new_range(m, n.id, n.id);
        let mut i = 0;

        while let Ok(Some((_, j))) = it.next_bool() {
            labels[i as usize] = j as i32;
            i += 1;
        }

        i
    }

    pub fn get_adjacency_matrix(
        &mut self,
        transposed: bool,
    ) -> &mut DeltaMatrix {
        let n = self.required_matrix_dim();
        let m = Graph::syncronize(self.matrix_policy, &mut self.adjacency_matrix, n, n);
        if transposed {
            m.transposed_mut().unwrap()
        } else {
            m
        }
    }

    pub fn get_label_matrix(
        &mut self,
        label: LabelID,
    ) -> &mut DeltaMatrix {
        if label < 0 {
            return self.get_zero_matrix();
        }
        let n = self.required_matrix_dim();
        Graph::syncronize(self.matrix_policy, &mut self.labels[label as usize], n, n)
    }

    pub fn get_relation_matrix(
        &mut self,
        relation_idx: i32,
        transposed: bool,
    ) -> &mut DeltaMatrix {
        let n = self.required_matrix_dim();
        let m = if relation_idx == -1 {
            &mut self.adjacency_matrix
        } else {
            &mut self.relations[relation_idx as usize].m
        };
        let m = Graph::syncronize(self.matrix_policy, m, n, n);
        if transposed {
            m.transposed_mut().unwrap()
        } else {
            m
        }
    }

    pub fn get_node_label_matrix(&mut self) -> &mut DeltaMatrix {
        let n = self.required_matrix_dim();
        Graph::syncronize(self.matrix_policy, &mut self.node_labels, n, n)
    }

    pub fn get_zero_matrix(&mut self) -> &mut DeltaMatrix {
        let n = self.required_matrix_dim();
        Graph::syncronize(self.matrix_policy, &mut self.zero_matrix, n, n)
    }

    fn syncronize(
        matrix_policy: MatrixPolicy,
        m: &mut DeltaMatrix,
        nrows: u64,
        ncols: u64,
    ) -> &mut DeltaMatrix {
        match matrix_policy {
            MatrixPolicy::FlushResize => m.synchronize(nrows, ncols),
            MatrixPolicy::Resize => m.resize(nrows, ncols),
            _ => (),
        };
        m
    }

    pub fn ensure_node_cap(
        &mut self,
        cap: u64,
    ) {
        unsafe { DataBlock_Ensure(self.nodes, cap) };

        let dim = unsafe { DataBlock_ItemCap(self.nodes) };

        self.get_adjacency_matrix(false).resize(dim, dim);

        self.get_node_label_matrix().resize(dim, dim);

        for i in 0..self.label_type_count() {
            self.get_label_matrix(i).resize(dim, dim);
        }

        for i in 0..self.relation_type_count() {
            self.get_relation_matrix(i, false).resize(dim, dim);
        }
    }

    pub fn mark_edge_deleted(
        &self,
        id: u64,
    ) {
        unsafe { DataBlock_MarkAsDeletedOutOfOrder(self.edges, id) };
    }

    pub fn mark_node_deleted(
        &self,
        id: u64,
    ) {
        unsafe { DataBlock_MarkAsDeletedOutOfOrder(self.nodes, id) };
    }

    pub fn set_node(
        &mut self,
        id: NodeID,
        labels: &[LabelID],
        n: &mut Node,
    ) {
        let set = unsafe { DataBlock_AllocateItemOutOfOrder(self.nodes, id) } as *mut AttributeSet;
        unsafe { set.write(null_mut()) };

        n.id = id;
        n.attributes = set;

        for label in labels {
            self.get_label_matrix(*label)
                .m_mut()
                .set_element_bool(true, id, id);
            self.stats.increment_node_count(*label, 1);
        }
    }

    pub fn set_edge(
        &mut self,
        multi_edge: bool,
        edge_id: u64,
        src: u64,
        dest: u64,
        r: i32,
        e: &mut Edge,
    ) {
        let set =
            unsafe { DataBlock_AllocateItemOutOfOrder(self.edges, edge_id) } as *mut AttributeSet;
        unsafe { set.write(null_mut()) };

        e.id = edge_id;
        e.src_id = src;
        e.dest_id = dest;
        e.attributes = set;
        e.relation_id = r;

        if multi_edge {
            self.form_connection(src, dest, r, e)
        } else {
            self.optimized_single_edge_form_connection(src, dest, edge_id, r);
        }
    }

    pub fn get_deleted_nodes_list(&self) -> *mut u64 {
        unsafe { DataBlock_DeletedItems(self.nodes) }
    }

    pub fn get_deleted_edges_list(&self) -> *mut u64 {
        unsafe { DataBlock_DeletedItems(self.edges) }
    }

    fn optimized_single_edge_form_connection(
        &mut self,
        src: u64,
        dest: u64,
        edge_id: u64,
        r: i32,
    ) {
        let m = self.get_relation_matrix(r, false);
        m.m_mut().set_element_u64(edge_id, src, dest);
        m.transposed_mut()
            .unwrap()
            .m_mut()
            .set_element_bool(true, dest, src);

        let adj = self.get_adjacency_matrix(false);
        adj.m_mut().set_element_bool(true, src, dest);
        adj.transposed_mut()
            .unwrap()
            .m_mut()
            .set_element_bool(true, dest, src);

        self.stats.increment_edge_count(r, 1);
    }

    pub fn set_node_labels(&mut self) {
        let mut v = MaybeUninit::uninit();
        let node_count = self.required_matrix_dim();
        let label_count = self.label_type_count();
        let node_labels = self.get_node_label_matrix();
        let node_labels_m = node_labels.m_mut().grb_matrix_ref();

        debug_assert!(node_labels.nvals() == 0);

        unsafe { GrB_Vector_new(v.as_mut_ptr(), GrB_BOOL, node_count) };

        let mut v = unsafe { v.assume_init() };

        for l in 0..label_count {
            let lm = self.get_label_matrix(l);
            let m = lm.m_mut();

            unsafe {
                grb_check!(GxB_Vector_diag(v, m.grb_matrix_ref(), 0, null_mut()));

                grb_check!(GxB_Row_subassign(
                    node_labels_m,
                    null_mut(),
                    null_mut(),
                    v,
                    l as u64,
                    GrB_ALL,
                    0,
                    null_mut(),
                ));
            };
        }

        unsafe {
            GrB_transpose(
                node_labels_m,
                null_mut(),
                null_mut(),
                node_labels_m,
                null_mut(),
            );

            GrB_Vector_free(&mut v);
        }
    }

    fn clear_adjacency_matrix_element(
        &mut self,
        r: RelationID,
        src: NodeID,
        dest: NodeID,
    ) {
        let mut connected = false;
        for ri in 0..self.relation_type_count() {
            if ri == r {
                continue;
            }

            if self
                .get_relation_matrix(ri as RelationID, false)
                .extract_element_bool(src, dest)
                .is_some()
            {
                connected = true;
                break;
            }
        }

        if !connected {
            self.get_adjacency_matrix(false).remove_element(src, dest);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use libc::c_char;

    use crate::{
        binding::graph::{ConfigOptionField, Config_Option_set, Edge},
        graph::{
            graph::GraphEdgeDir,
            matrix::GraphBLAS::{
                GrB_Mode, GrB_init, GxB_Format_Value, GxB_Global_Option_set, GxB_Option_Field,
            },
        },
    };

    use super::Graph;

    extern "C" {
        fn Alloc_Reset();
    }
    fn test_init() {
        unsafe {
            Alloc_Reset();
            GrB_init(GrB_Mode::GrB_NONBLOCKING);
            GxB_Global_Option_set(GxB_Option_Field::GxB_FORMAT, GxB_Format_Value::GxB_BY_ROW);
            Config_Option_set(
                ConfigOptionField::DELTA_MAX_PENDING_CHANGES,
                "10000\0".as_ptr() as *const c_char,
                null_mut(),
            );
        };
    }

    #[test]
    fn test_new_graph() {
        test_init();
        let mut g = Graph::new(16384, 16384);
        g.acquire_write_lock();

        assert_eq!(g.adjacency_matrix.ncols(), g.required_matrix_dim());
        assert_eq!(g.adjacency_matrix.nrows(), g.required_matrix_dim());
        assert_eq!(g.adjacency_matrix.nvals(), 0);

        assert_eq!(g.node_count(), 0);

        g.release_lock();
    }

    #[test]
    fn test_graph_constructions() {
        test_init();
        let node_count = 16384 / 2;
        let mut g = Graph::new(node_count, node_count);
        g.acquire_write_lock();

        for _ in 0..node_count {
            let mut n = g.reserve_node();
            g.create_node(&mut n, &[]);
        }

        assert_eq!(g.node_count(), node_count as u64);
        assert!(g.adjacency_matrix.ncols() >= node_count);
        assert!(g.adjacency_matrix.nrows() >= node_count);
        assert_eq!(g.adjacency_matrix.nvals(), 0);

        g.release_lock();
    }

    #[test]
    fn test_remove_nodes() {
        test_init();
        let mut g = Graph::new(32, 32);
        g.acquire_write_lock();

        let mut n1 = g.reserve_node();
        g.create_node(&mut n1, &[]);
        let mut n2 = g.reserve_node();
        g.create_node(&mut n2, &[]);
        let mut n3 = g.reserve_node();
        g.create_node(&mut n3, &[]);

        let r = g.add_relation_type();
        let mut edge = Edge {
            src_id: 0,
            dest_id: 1,
            id: 0,
            attributes: null_mut(),
            relation_id: r,
            relationship: null_mut(),
        };

        g.create_edge(0, 1, r, &mut edge);
        g.create_edge(1, 0, r, &mut edge);
        g.create_edge(1, 2, r, &mut edge);

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.get_relation_matrix(r, false).nvals(), 3);
        assert_eq!(g.get_adjacency_matrix(false).nvals(), 3);

        let mut edges = Vec::new();
        g.get_node_edges(&n1, GraphEdgeDir::Both, -1, &mut edges);
        g.delete_edges(edges.as_mut_slice());
        g.delete_nodes(&[n1]);

        g.release_lock();

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn test_get_node() {
        test_init();
        let mut g = Graph::new(16, 16);
        g.acquire_write_lock();
        for _ in 0..16 {
            let mut n = g.reserve_node();
            g.create_node(&mut n, &[]);
        }
        g.release_lock();

        for i in 0..16 {
            let n = g.get_node(i).unwrap();
            assert!(!n.attributes.is_null());
        }
    }

    #[test]
    fn test_get_edge() {
        test_init();
        let mut g = Graph::new(5, 55);
        g.acquire_write_lock();
        for _ in 0..5 {
            let mut n = g.reserve_node();
            g.create_node(&mut n, &[]);
        }
        let mut relations = [0; 4];
        for i in 0..4 {
            relations[i] = g.add_relation_type();
        }

        let mut e = Edge {
            src_id: 0,
            dest_id: 1,
            id: 0,
            attributes: null_mut(),
            relation_id: relations[0],
            relationship: null_mut(),
        };
        g.create_edge(0, 1, relations[0], &mut e);
        g.create_edge(0, 1, relations[1], &mut e);
        g.create_edge(1, 2, relations[1], &mut e);
        g.create_edge(2, 3, relations[2], &mut e);
        g.create_edge(3, 4, relations[3], &mut e);
        g.release_lock();

        for i in 0..5 {
            let mut e = Edge {
                src_id: 0,
                dest_id: 0,
                id: 0,
                attributes: null_mut(),
                relation_id: 0,
                relationship: null_mut(),
            };
            g.get_edge(i, &mut e);
            assert_eq!(e.id, i);
            assert!(!e.attributes.is_null());
        }
    }
}
