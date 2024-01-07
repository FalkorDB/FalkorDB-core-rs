use std::{ffi::c_char, slice::from_raw_parts};

use libc::strlen;

use crate::binding::{
    constraint::{Constraint, ConstraintType},
    graph::{AttributeID, Edge, GraphEntity, IndexFieldType, Node},
    index::{Index, IndexField},
};

use super::schema::{Schema, SchemaType};

type _Schema = *mut Schema;

#[no_mangle]
unsafe extern "C" fn Schema_New(
    typ: SchemaType,
    id: i32,
    name: *const c_char,
) -> _Schema {
    Box::into_raw(Box::new(Schema::new(typ, id, name)))
}

#[no_mangle]
unsafe extern "C" fn Schema_GetName(s: _Schema) -> *const c_char {
    s.as_ref().unwrap().get_name()
}

#[no_mangle]
unsafe extern "C" fn Schema_GetID(s: _Schema) -> i32 {
    s.as_ref().unwrap().get_id()
}

#[no_mangle]
unsafe extern "C" fn Schema_GetType(s: _Schema) -> SchemaType {
    s.as_ref().unwrap().get_type()
}

#[no_mangle]
unsafe extern "C" fn Schema_HasIndices(s: _Schema) -> bool {
    s.as_ref().unwrap().has_indices()
}

#[no_mangle]
unsafe extern "C" fn Schema_GetIndicies(
    s: _Schema,
    indicies: *mut Index,
) -> u16 {
    let mut i = 0;
    let s = s.as_ref().unwrap();

    if !s.get_active_index().is_null() {
        indicies.write(s.get_active_index());
        i += 1;
    }

    if !s.get_pending_index().is_null() {
        indicies.add(i).write(s.get_active_index());
        i += 1;
    }

    i as u16
}

#[no_mangle]
unsafe extern "C" fn Schema_GetIndex(
    s: _Schema,
    attrs: *const AttributeID,
    n: usize,
    t: IndexFieldType,
    include_pending: bool,
) -> Index {
    s.as_ref()
        .unwrap()
        .get_index(from_raw_parts(attrs, n).to_vec(), t, include_pending)
}

#[no_mangle]
unsafe extern "C" fn Schema_GetActiveIndex(s: _Schema) -> Index {
    s.as_ref().unwrap().get_active_index()
}

#[no_mangle]
unsafe extern "C" fn Schema_GetPendingIndex(s: _Schema) -> Index {
    s.as_ref().unwrap().get_pending_index()
}

#[no_mangle]
unsafe extern "C" fn Schema_AddIndex(
    s: _Schema,
    idx: *mut Index,
    fields: *mut IndexField,
    n: usize,
) -> bool {
    s.as_mut()
        .unwrap()
        .add_index(idx, from_raw_parts(fields, n).to_vec())
}

#[no_mangle]
unsafe extern "C" fn Schema_RemoveIndex(
    s: _Schema,
    f: *const c_char,
    t: IndexFieldType,
) -> bool {
    s.as_mut().unwrap().remove_index(f, t)
}

#[no_mangle]
unsafe extern "C" fn Schema_ActivateIndex(s: _Schema) {
    s.as_mut().unwrap().activate_index()
}

#[no_mangle]
unsafe extern "C" fn Schema_AddNodeToIndex(
    s: _Schema,
    n: *const Node,
) {
    s.as_mut().unwrap().add_node_to_index(n)
}

#[no_mangle]
unsafe extern "C" fn Schema_AddEdgeToIndex(
    s: _Schema,
    e: *const Edge,
) {
    s.as_mut().unwrap().add_edge_to_index(e)
}

#[no_mangle]
unsafe extern "C" fn Schema_RemoveNodeFromIndex(
    s: _Schema,
    n: *const Node,
) {
    s.as_mut().unwrap().remove_node_from_index(n)
}

#[no_mangle]
unsafe extern "C" fn Schema_RemoveEdgeFromIndex(
    s: _Schema,
    e: *const Edge,
) {
    s.as_mut().unwrap().remove_edge_from_index(e)
}

#[no_mangle]
unsafe extern "C" fn Schema_Free(s: _Schema) {
    drop(Box::from_raw(s))
}

#[no_mangle]
unsafe extern "C" fn Schema_HasConstraints(s: _Schema) -> bool {
    s.as_ref().unwrap().has_constraints()
}

#[no_mangle]
unsafe extern "C" fn Schema_ContainsConstraint(
    s: _Schema,
    t: ConstraintType,
    attrs: *const AttributeID,
    attr_count: u32,
) -> bool {
    s.as_ref()
        .unwrap()
        .contains_constraint(t, from_raw_parts(attrs, attr_count as usize).to_vec())
}

#[no_mangle]
unsafe extern "C" fn Schema_GetConstraint(
    s: _Schema,
    t: ConstraintType,
    attrs: *const AttributeID,
    attr_count: u32,
) -> Constraint {
    s.as_ref()
        .unwrap()
        .get_constraint(t, from_raw_parts(attrs, attr_count as usize).to_vec())
}

#[no_mangle]
unsafe extern "C" fn Schema_GetConstraints(s: _Schema) -> *const Constraint {
    s.as_ref().unwrap().get_constraints().as_ptr()
}

#[no_mangle]
unsafe extern "C" fn Schema_AddConstraint(
    s: _Schema,
    c: Constraint,
) {
    s.as_mut().unwrap().add_constraint(c)
}

#[no_mangle]
unsafe extern "C" fn Schema_RemoveConstraint(
    s: _Schema,
    c: Constraint,
) {
    s.as_mut().unwrap().remove_constraint(c)
}

#[no_mangle]
unsafe extern "C" fn Schema_EnforceConstraints(
    s: _Schema,
    e: *const GraphEntity,
    err_msg: *mut *mut c_char,
) -> bool {
    s.as_mut().unwrap().enforce_constraints(e, err_msg)
}
