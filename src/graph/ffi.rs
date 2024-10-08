/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{
    mem::size_of,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::{
    binding::graph::{
        DataBlockIterator, DataBlock_ItemIsDeleted, Edge, EdgeID, GraphEntity, LabelID, Node,
        NodeID, RelationID,
    },
    RedisModule_Realloc,
};

use super::{
    graph::{Graph, GraphEdgeDir, MatrixPolicy},
    matrix::{
        delta_matrix::DeltaMatrix,
        tensor::{Tensor, TensorRangeIterator},
    },
};

#[repr(C)]
struct ArrayHeader {
    len: u32,
    cap: u32,
    elem_sz: u32,
    pad: u32,
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AcquireReadLock(g: *mut Graph) {
    (&mut *g).acquire_read_lock();
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AcquireWriteLock(g: *mut Graph) {
    (&mut *g).acquire_write_lock();
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ReleaseLock(g: *mut Graph) {
    (&mut *g).release_lock();
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ApplyAllPending(
    g: *mut Graph,
    force_flush: bool,
) {
    (&mut *g).apply_all_pending(force_flush);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetMatrixPolicy(g: *mut Graph) -> MatrixPolicy {
    (&*g).get_matrix_policy()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_SetMatrixPolicy(
    g: *mut Graph,
    policy: MatrixPolicy,
) -> MatrixPolicy {
    (&mut *g).set_matrix_policy(policy)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_Pending(g: *mut Graph) -> bool {
    (&*g).pending()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_New(
    node_cap: u64,
    edge_cap: u64,
) -> *mut Graph {
    Box::into_raw(Box::new(Graph::new(node_cap, edge_cap)))
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AddLabel(g: *mut Graph) -> LabelID {
    (&mut *g).add_label()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_LabelNode(
    g: *mut Graph,
    id: NodeID,
    lbls: *mut LabelID,
    lbl_count: u32,
) {
    (&mut *g).label_node(id, from_raw_parts(lbls, lbl_count as usize));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_RemoveNodeLabels(
    g: *mut Graph,
    id: NodeID,
    lbls: *mut LabelID,
    lbl_count: u32,
) {
    (&mut *g).remove_node_labels(id, from_raw_parts(lbls, lbl_count as usize));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_IsNodeLabeled(
    g: *mut Graph,
    id: NodeID,
    l: LabelID,
) -> bool {
    (&mut *g).is_node_labeled(id, l)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AddRelationType(g: *mut Graph) -> RelationID {
    (&mut *g).add_relation_type()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AllocateNodes(
    g: *mut Graph,
    n: usize,
) {
    (&*g).allocate_nodes(n);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_AllocateEdges(
    g: *mut Graph,
    n: usize,
) {
    (&*g).allocate_edges(n);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ResetReservedNode(g: *mut Graph) {
    (&mut *g).reset_reserved_node();
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ReserveNode(g: *mut Graph) -> Node {
    (&mut *g).reserve_node()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_CreateNode(
    g: *mut Graph,
    n: *mut Node,
    labels: *mut LabelID,
    label_count: u32,
) {
    (&mut *g).create_node(
        n.as_mut().unwrap(),
        if label_count == 0 {
            &[]
        } else {
            from_raw_parts(labels, label_count as usize)
        },
    );
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_CreateEdge(
    g: *mut Graph,
    src: NodeID,
    dest: NodeID,
    r: RelationID,
    e: *mut Edge,
) {
    (&mut *g).create_edge(src, dest, r, e.as_mut().unwrap());
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_CreateEdges(
    g: *mut Graph,
    r: RelationID,
    edges: *mut *mut Edge,
    count: u32,
) {
    let edges = from_raw_parts_mut(edges, count as usize);
    (&mut *g).create_edges(r, edges);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_DeleteNodes(
    g: *mut Graph,
    nodes: *mut Node,
    count: u64,
) {
    (&mut *g).delete_nodes(from_raw_parts(nodes, count as usize));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_DeleteEdges(
    g: *mut Graph,
    edges: *mut Edge,
    count: u64,
) {
    (&mut *g).delete_edges(from_raw_parts_mut(edges, count as usize));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_EntityIsDeleted(ge: *mut GraphEntity) -> bool {
    if (&*ge).attributes.is_null() {
        return false;
    }

    DataBlock_ItemIsDeleted((&*ge).attributes as _)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_RequiredMatrixDim(g: *mut Graph) -> u64 {
    (&*g).required_matrix_dim()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ScanNodes(g: *mut Graph) -> *mut DataBlockIterator {
    (&*g).scan_nodes()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_ScanEdges(g: *mut Graph) -> *mut DataBlockIterator {
    (&*g).scan_edges()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_NodeCount(g: *mut Graph) -> u64 {
    (&*g).node_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_DeletedNodeCount(g: *mut Graph) -> u64 {
    (&*g).deleted_node_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_UncompactedNodeCount(g: *mut Graph) -> u64 {
    (&*g).uncompacted_node_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_LabeledNodeCount(
    g: *mut Graph,
    label: i32,
) -> u64 {
    (&*g).labeled_node_count(label)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_EdgeCount(g: *mut Graph) -> usize {
    (&*g).edge_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_RelationEdgeCount(
    g: *mut Graph,
    relation_idx: i32,
) -> u64 {
    (&*g).relation_edge_count(relation_idx)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_DeletedEdgeCount(g: *mut Graph) -> u32 {
    (&*g).deleted_edge_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_RelationTypeCount(g: *mut Graph) -> i32 {
    (&*g).relation_type_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_LabelTypeCount(g: *mut Graph) -> i32 {
    (&*g).label_type_count()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_RelationshipContainsMultiEdge(
    g: *mut Graph,
    r: RelationID,
) -> bool {
    (&mut *g).relationship_contains_multi_edge(r)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetNode(
    g: *mut Graph,
    id: NodeID,
    n: *mut Node,
) -> bool {
    match (&*g).get_node(id) {
        Some(node) => {
            n.write(node);
            true
        }
        None => false,
    }
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetEdge(
    g: *mut Graph,
    id: EdgeID,
    e: *mut Edge,
) -> bool {
    (&*g).get_edge(id, e.as_mut().unwrap())
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetEdgesConnectingNodes(
    g: *mut Graph,
    src_id: NodeID,
    dest_id: NodeID,
    r: RelationID,
    edges: *mut *mut Edge,
) {
    let es = (&mut *g).get_edges_connecting_nodes(src_id, dest_id, r);
    let mut arr_ptr = (*edges as *mut ArrayHeader).sub(1);
    let mut arr = arr_ptr.as_mut().unwrap();
    if arr.cap - arr.len < es.len() as u32 {
        arr.cap = arr.len + es.len() as u32;
        arr_ptr = RedisModule_Realloc.unwrap()(
            arr_ptr as _,
            (arr.cap as usize * size_of::<Edge>() + size_of::<ArrayHeader>()) as usize,
        ) as _;
        edges.write(arr_ptr.add(1) as _);
        arr = arr_ptr.as_mut().unwrap();
    }
    for (i, e) in es.iter().enumerate() {
        (*edges).add(arr.len as usize + i).write(*e);
    }
    arr.len += es.len() as u32;
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetNodeEdges(
    g: *mut Graph,
    n: *const Node,
    dir: GraphEdgeDir,
    edge_type: RelationID,
    edges: *mut *mut Edge,
) {
    let mut es = Vec::new();
    (&mut *g).get_node_edges(n.as_ref().unwrap(), dir, edge_type, &mut es);
    let mut arr_ptr = (*edges as *mut ArrayHeader).sub(1);
    let mut arr = arr_ptr.as_mut().unwrap();
    if arr.cap - arr.len < es.len() as u32 {
        arr.cap = arr.len + es.len() as u32;
        arr_ptr = RedisModule_Realloc.unwrap()(
            arr_ptr as _,
            (arr.cap as usize * size_of::<Edge>() + size_of::<ArrayHeader>()) as usize,
        ) as _;
        edges.write(arr_ptr.add(1) as _);
        arr = arr_ptr.as_mut().unwrap();
    }
    for (i, e) in es.iter().enumerate() {
        (*edges).add(arr.len as usize + i).write(*e);
    }
    arr.len += es.len() as u32;
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetNodeDegree(
    g: *mut Graph,
    n: *const Node,
    dir: GraphEdgeDir,
    edge_type: RelationID,
) -> u64 {
    (&mut *g).get_node_degree(n.as_ref().unwrap(), dir, edge_type)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetNodeLabels(
    g: *mut Graph,
    n: *const Node,
    labels: *mut LabelID,
    label_count: u32,
) -> u32 {
    (&mut *g).get_node_labels(
        n.as_ref().unwrap(),
        from_raw_parts_mut(labels, label_count as usize),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetAdjacencyMatrix(
    g: *mut Graph,
    transposed: bool,
) -> *mut DeltaMatrix {
    (&mut *g).get_adjacency_matrix(transposed)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetLabelMatrix(
    g: *mut Graph,
    label: LabelID,
) -> *mut DeltaMatrix {
    (&mut *g).get_label_matrix(label)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetRelationMatrix(
    g: *mut Graph,
    relation_idx: RelationID,
    transposed: bool,
) -> *mut DeltaMatrix {
    (&mut *g).get_relation_matrix(relation_idx, transposed)
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetNodeLabelMatrix(g: *mut Graph) -> *mut DeltaMatrix {
    (&mut *g).get_node_label_matrix()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetZeroMatrix(g: *mut Graph) -> *mut DeltaMatrix {
    (&mut *g).get_zero_matrix()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_PartialFree(g: *mut Graph) {
    (&mut *g).set_partial();
    drop(Box::from_raw(g));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_Free(g: *mut Graph) {
    drop(Box::from_raw(g));
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_EnsureNodeCap(
    g: *mut Graph,
    cap: u64,
) {
    (&mut *g).ensure_node_cap(cap);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_MarkEdgeDeleted(
    g: *mut Graph,
    id: EdgeID,
) {
    (&mut *g).mark_edge_deleted(id);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_MarkNodeDeleted(
    g: *mut Graph,
    id: EdgeID,
) {
    (&mut *g).mark_node_deleted(id);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_SetNode(
    g: *mut Graph,
    id: NodeID,
    labels: *const LabelID,
    label_count: u32,
    n: *mut Node,
) {
    (&mut *g).set_node(
        id,
        from_raw_parts(labels, label_count as usize),
        n.as_mut().unwrap(),
    );
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_SetNodeLabels(g: *mut Graph) {
    (&mut *g).set_node_labels();
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_SetEdge(
    g: *mut Graph,
    multi_edge: bool,
    edge_id: EdgeID,
    src: NodeID,
    dest: NodeID,
    r: RelationID,
    e: *mut Edge,
) {
    (&mut *g).set_edge(multi_edge, edge_id, src, dest, r, e.as_mut().unwrap());
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetDeletedNodesList(g: *mut Graph) -> *mut u64 {
    (&*g).get_deleted_nodes_list()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn Graph_GetDeletedEdgesList(g: *mut Graph) -> *mut u64 {
    (&*g).get_deleted_edges_list()
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn TensorIterator_ScanRange(
    it: *mut TensorRangeIterator<'static>,
    m: *mut Tensor,
    min_src_id: NodeID,
    max_src_id: NodeID,
    transposed: bool,
) {
    let iter = m
        .as_ref()
        .unwrap()
        .iter_range(min_src_id, max_src_id, transposed);
    it.write(iter);
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn TensorIterator_next(
    it: *mut TensorRangeIterator<'static>,
    src: *mut NodeID,
    dest: *mut NodeID,
    edge_id: *mut EdgeID,
) -> bool {
    if let Some((src_id, dest_id, id)) = it.as_mut().unwrap().next() {
        if !src.is_null() {
            src.write(src_id);
        }
        if !dest.is_null() {
            dest.write(dest_id);
        }
        if !edge_id.is_null() {
            edge_id.write(id);
        }
        true
    } else {
        false
    }
}

#[no_mangle]
#[allow(non_snake_case)]
unsafe extern "C" fn TensorIterator_is_attached(
    it: *const TensorRangeIterator<'static>,
    m: *mut Tensor,
) -> bool {
    it.as_ref().unwrap().is_attached(m.as_ref().unwrap())
}
