/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::UndoLog;
use crate::attribute_set::AttributeSet;
use crate::graph_context::GraphContext;
use crate::graph_entity::edge::Edge;
use crate::graph_entity::node::Node;
use crate::index::IndexFieldType;
use crate::schema::SchemaType;
use crate::types::{AttributeID, LabelID};
use std::{ffi::c_char, slice::from_raw_parts};

#[no_mangle]
unsafe extern "C" fn UndoLog_New() -> *mut UndoLog {
    Box::into_raw(Box::new(UndoLog::new()))
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateNode(
    log: *mut UndoLog,
    node: *const Node,
) {
    (*log).create_node(node.read());
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateEdge(
    log: *mut UndoLog,
    edge: *const Edge,
) {
    (*log).create_edge(edge.read());
}

#[no_mangle]
unsafe extern "C" fn UndoLog_DeleteNode(
    log: *mut UndoLog,
    node: *const Node,
    labels: *const LabelID,
    labels_count: usize,
) {
    let n = node.read();
    let set = n.attributes.read_unaligned();
    n.attributes
        .write((set as u64 | (1u64 << (u64::BITS as usize - 1))) as *mut _);
    log.as_mut()
        .unwrap()
        .delete_node(n.id, set, from_raw_parts(labels, labels_count).to_vec());
}

#[no_mangle]
unsafe extern "C" fn UndoLog_DeleteEdge(
    log: *mut UndoLog,
    edge: *const Edge,
) {
    let e = edge.read();
    let set = e.attributes.read_unaligned();
    e.attributes
        .write((set as u64 | (1u64 << (u64::BITS as usize - 1))) as *mut _);
    log.as_mut()
        .unwrap()
        .delete_edge(e.id, e.src_id, e.dest_id, e.relation_id, set);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_UpdateNode(
    log: *mut UndoLog,
    node: *const Node,
    old_set: AttributeSet,
) {
    (*log).update_node(node.read(), old_set);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_UpdateEdge(
    log: *mut UndoLog,
    edge: *const Edge,
    old_set: AttributeSet,
) {
    (*log).update_edge(edge.read(), old_set);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddLabels(
    log: *mut UndoLog,
    node: *const Node,
    label_ids: *const LabelID,
    labels_count: usize,
) {
    (*log).add_labels(
        node.read(),
        from_raw_parts(label_ids, labels_count).to_vec(),
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_RemoveLabels(
    log: *mut UndoLog,
    node: *const Node,
    label_ids: *const LabelID,
    labels_count: usize,
) {
    (*log).remove_labels(
        node.read(),
        from_raw_parts(label_ids, labels_count).to_vec(),
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddSchema(
    log: *mut UndoLog,
    schema_id: i32,
    t: SchemaType,
) {
    (*log).add_schema(schema_id, t);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddAttribute(
    log: *mut UndoLog,
    attribute_id: AttributeID,
) {
    (*log).add_attribute(attribute_id);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateIndex(
    log: *mut UndoLog,
    st: SchemaType,
    label: *const c_char,
    field: *const c_char,
    t: IndexFieldType,
) {
    (*log).create_index(st, label, field, t);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_Rollback(
    log: *mut UndoLog,
    gc: *mut GraphContext,
) {
    (*log).rollback(&mut (*gc));
    drop(Box::from_raw(log));
}

#[no_mangle]
unsafe extern "C" fn UndoLog_Free(log: *mut UndoLog) {
    drop(Box::from_raw(log));
}
