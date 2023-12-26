/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{ffi::{c_void, c_char}, slice::from_raw_parts};

use super::undo_log::*;

type UndoLog = *mut c_void;

#[no_mangle]
unsafe extern "C" fn UndoLog_New() -> UndoLog {
    Box::into_raw(Box::new(_UndoLog::new())) as *mut _
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateNode(
    log: UndoLog,
    node: *const Node,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .create_node(node.read());
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateEdge(
    log: UndoLog,
    edge: *const Edge,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .create_edge(edge.read());
}

#[no_mangle]
unsafe extern "C" fn UndoLog_DeleteNode(
    log: UndoLog,
    node: *const Node,
    labels: *const LabelID,
    labels_count: usize,
) {
    let n = node.read();
    let set = n.attributes.read_unaligned();
    n.attributes
        .write((set as u64 | (1u64 << (u64::BITS as usize - 1))) as *mut _);
    (log as *mut _UndoLog).as_mut().unwrap().delete_node(
        n.id,
        set,
        from_raw_parts(labels, labels_count).to_vec(),
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_DeleteEdge(
    log: UndoLog,
    edge: *const Edge,
) {
    let e = edge.read();
    let set = e.attributes.read_unaligned();
    e.attributes
        .write((set as u64 | (1u64 << (u64::BITS as usize - 1))) as *mut _);
    (log as *mut _UndoLog).as_mut().unwrap().delete_edge(
        e.id,
        e.src_id,
        e.dest_id,
        e.relation_id,
        set,
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_UpdateNode(
    log: UndoLog,
    node: *const Node,
    old_set: AttributeSet,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .update_node(node.read(), old_set);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_UpdateEdge(
    log: UndoLog,
    edge: *const Edge,
    old_set: AttributeSet,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .update_edge(edge.read(), old_set);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddLabels(
    log: UndoLog,
    node: *const Node,
    label_ids: *const LabelID,
    labels_count: usize,
) {
    (log as *mut _UndoLog).as_mut().unwrap().add_labels(
        node.read(),
        from_raw_parts(label_ids, labels_count).to_vec(),
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_RemoveLabels(
    log: UndoLog,
    node: *const Node,
    label_ids: *const LabelID,
    labels_count: usize,
) {
    (log as *mut _UndoLog).as_mut().unwrap().remove_labels(
        node.read(),
        from_raw_parts(label_ids, labels_count).to_vec(),
    );
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddSchema(
    log: UndoLog,
    schema_id: i32,
    t: SchemaType,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .add_schema(schema_id, t);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_AddAttribute(
    log: UndoLog,
    attribute_id: AttributeID,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .add_attribute(attribute_id);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_CreateIndex(
    log: UndoLog,
    st: SchemaType,
    label: *const c_char,
    field: *const c_char,
    t: IndexFieldType,
) {
    (log as *mut _UndoLog)
        .as_mut()
        .unwrap()
        .create_index(st, label, field, t);
}

#[no_mangle]
unsafe extern "C" fn UndoLog_Rollback(
    log: UndoLog,
    gc: *mut GraphContext,
    g: *mut Graph,
) {
    (log as *mut _UndoLog).as_mut().unwrap().rollback(gc, g);
    drop(Box::from_raw(log as *mut _UndoLog));
}

#[no_mangle]
unsafe extern "C" fn UndoLog_Free(log: UndoLog) {
    drop(Box::from_raw(log as *mut _UndoLog));
}
