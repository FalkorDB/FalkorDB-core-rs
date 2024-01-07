use std::ffi::{c_void, c_char};

use super::graph::{AttributeID, IndexFieldType, Node, Edge};

pub type Index = *mut c_void;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IndexField {
    name: *const c_char,
	id: AttributeID,
	typ: IndexFieldType,
	weight: f64,
	nostem:	bool,
	phonetic: *const c_char,
	dimension: u32,
	range_name: *const c_char,
	vector_name: *const c_char,
	fulltext_name: *const c_char,
}

extern "C" {
    pub fn Index_ContainsField(
        index: Index,
        attr: AttributeID,
    ) -> bool;
	pub fn Index_IndexNode(
		index: Index,
		node: *const Node,
	);
	pub fn Index_IndexEdge(
		index: Index,
		edge: *const Edge,
	);
	pub fn Index_RemoveNode(
		index: Index,
		node: *const Node,
	);
	pub fn Index_RemoveEdge(
		index: Index,
		edge: *const Edge,
	);
	pub fn Index_Free(index: Index);
}
