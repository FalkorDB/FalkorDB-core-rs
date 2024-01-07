use std::{ffi::{c_void, c_char}, ptr::null_mut, slice::from_raw_parts};

use super::graph::{AttributeID, GraphEntity};

#[repr(C)]
#[derive(PartialEq)]
pub enum ConstraintType {
    Unique,
    Mandatory,
}

#[repr(C)]
#[derive(PartialEq)]
pub enum ConstraintStatus {
    Active = 0,
    Pending,
    Failed,
}

pub type Constraint = *mut c_void;

pub struct ConstraintAPI {
    c: Constraint,
}

impl ConstraintAPI {
    pub fn new(c: Constraint) -> Self {
        Self { c }
    }

    pub fn get_status(&self) -> ConstraintStatus {
        unsafe { Constraint_GetStatus(self.c) }
    }

    pub fn get_type(&self) -> ConstraintType {
        unsafe { Constraint_GetType(self.c) }
    }

    pub fn get_attributes(&self) -> Vec<AttributeID> {
        let mut attr_ids: *const AttributeID = std::ptr::null();
        let attr_count = unsafe { Constraint_GetAttributes(self.c, &mut attr_ids, null_mut()) };

        unsafe { from_raw_parts(attr_ids, attr_count as usize).to_vec() }
    }
}

extern "C" {
    pub fn Constraint_GetStatus(c: Constraint) -> ConstraintStatus;
    pub fn Constraint_GetType(c: Constraint) -> ConstraintType;
    pub fn Constraint_GetAttributes(
        c: Constraint,
        attr_ids: *mut *const AttributeID,
        attr_names: *mut *mut *const c_char,
    ) -> u8;
	pub fn Constraint_EnforceEntity(
		c: Constraint,
		entity: *const GraphEntity,
		err_msg: *mut *mut c_char
	) -> bool;
	pub fn Constraint_SetPrivateData(
		c: Constraint,
		data: *mut c_void,
	);
}
