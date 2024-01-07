use std::{ffi::c_char, ptr::null_mut};

use crate::binding::{
    constraint::{
        Constraint, ConstraintAPI, ConstraintStatus, ConstraintType, Constraint_EnforceEntity,
        Constraint_GetStatus, Constraint_GetType, Constraint_SetPrivateData,
    },
    graph::{AttributeID, Edge, GraphEntity, IndexFieldType, Node},
    index::{
        Index, IndexField, Index_ContainsField, Index_IndexEdge, Index_IndexNode, Index_RemoveEdge,
        Index_RemoveNode, Index_Free,
    },
};

#[repr(C)]
#[derive(Copy, Clone, PartialEq)]
pub enum SchemaType {
    Node,
    Edge,
}

pub struct Schema {
    id: i32,
    name: *const c_char,
    typ: SchemaType,
    active_index: Index,
    pending_index: Index,
    constraints: Vec<Constraint>,
}

impl Schema {
    pub fn new(
        typ: SchemaType,
        id: i32,
        name: *const c_char,
    ) -> Self {
        Self {
            id,
            name,
            typ,
            active_index: null_mut(),
            pending_index: null_mut(),
            constraints: Vec::new(),
        }
    }

    pub fn get_name(&self) -> *const c_char {
        self.name
    }

    pub fn get_id(&self) -> i32 {
        self.id
    }

    pub fn get_type(&self) -> SchemaType {
        self.typ
    }

    pub fn has_indices(&self) -> bool {
        !self.get_active_index().is_null() || !self.get_pending_index().is_null()
    }

    pub fn get_index(
        &self,
        attrs: Vec<AttributeID>,
        field: IndexFieldType,
        include_pending: bool,
    ) -> Index {
        if !self.active_index.is_null() {
            for attr in attrs {
                if !unsafe { Index_ContainsField(self.active_index, attr) } {
                    return null_mut();
                }
            }
            return self.active_index;
        } else if !self.pending_index.is_null() && include_pending {
            for attr in attrs {
                if !unsafe { Index_ContainsField(self.pending_index, attr) } {
                    return null_mut();
                }
            }
            return self.pending_index;
        }
        null_mut()
    }

    pub fn get_active_index(&self) -> Index {
        self.active_index
    }

    pub fn get_pending_index(&self) -> Index {
        self.pending_index
    }

    pub fn get_constraints(&self) -> &Vec<Constraint> {
        &self.constraints
    }

    pub(crate) fn activate_index(&mut self) {
        if !self.active_index.is_null() {
            unsafe { Index_Free(self.active_index) };
        }
    
        self.active_index = self.pending_index;
    
        self.pending_index = null_mut();

        for c in self.constraints.iter() {
            unsafe { Constraint_SetPrivateData(*c, self.active_index) };
        }
    }

    pub(crate) fn remove_index(
        &self,
        f: *const i8,
        t: IndexFieldType,
    ) -> bool {
        todo!()
    }

    pub(crate) fn add_index(
        &self,
        idx: *mut Index,
        fields: Vec<IndexField>,
    ) -> bool {
        todo!()
    }

    pub(crate) fn add_node_to_index(
        &self,
        n: *const Node,
    ) {
        if !self.active_index.is_null() {
            unsafe {
                Index_IndexNode(self.active_index, n);
            }
        }
        if !self.pending_index.is_null() {
            unsafe {
                Index_IndexNode(self.pending_index, n);
            }
        }
    }

    pub(crate) fn add_edge_to_index(
        &self,
        e: *const Edge,
    ) {
        if !self.active_index.is_null() {
            unsafe {
                Index_IndexEdge(self.active_index, e);
            }
        }
        if !self.pending_index.is_null() {
            unsafe {
                Index_IndexEdge(self.pending_index, e);
            }
        }
    }

    pub(crate) fn remove_node_from_index(
        &self,
        n: *const Node,
    ) {
        if !self.active_index.is_null() {
            unsafe {
                Index_RemoveNode(self.active_index, n);
            }
        }
        if !self.pending_index.is_null() {
            unsafe {
                Index_RemoveNode(self.pending_index, n);
            }
        }
    }

    pub(crate) fn remove_edge_from_index(
        &self,
        e: *const Edge,
    ) {
        if !self.active_index.is_null() {
            unsafe {
                Index_RemoveEdge(self.active_index, e);
            }
        }
        if !self.pending_index.is_null() {
            unsafe {
                Index_RemoveEdge(self.pending_index, e);
            }
        }
    }

    pub(crate) fn has_constraints(&self) -> bool {
        !self.constraints.is_empty()
    }

    pub(crate) fn contains_constraint(
        &self,
        t: ConstraintType,
        attrs: Vec<AttributeID>,
    ) -> bool {
        let c = self.get_constraint(t, attrs);
        !c.is_null() && unsafe { Constraint_GetStatus(c) } != ConstraintStatus::Failed
    }

    pub(crate) fn get_constraint(
        &self,
        t: ConstraintType,
        attrs: Vec<AttributeID>,
    ) -> Constraint {
        for c in self
            .constraints
            .iter()
            .filter(|c| unsafe { Constraint_GetType(**c) } != t)
        {
            let c_attrs = ConstraintAPI::new(*c).get_attributes();
            if attrs.len() != c_attrs.len() {
                continue;
            }

            if attrs == c_attrs {
                return *c;
            }
        }

        null_mut()
    }

    pub(crate) fn add_constraint(
        &mut self,
        c: Constraint,
    ) {
        self.constraints.push(c);
    }

    pub(crate) fn remove_constraint(
        &mut self,
        c: Constraint,
    ) {
        self.constraints.retain(|x| *x != c);
    }

    pub(crate) fn enforce_constraints(
        &self,
        e: *const GraphEntity,
        err_msg: *mut *mut c_char,
    ) -> bool {
        for c in self
            .constraints
            .iter()
            .filter(|c| unsafe { Constraint_GetStatus(**c) != ConstraintStatus::Failed })
        {
            if !unsafe { Constraint_EnforceEntity(*c, e, err_msg) } {
                // entity failed to pass constraint
                return false;
            }
        }
        true
    }
}
