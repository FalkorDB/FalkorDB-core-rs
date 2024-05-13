use crate::attribute_set::ffi::AttributeSet_Free;
use crate::attribute_set::AttributeSet;
use crate::types::{EntityID, NodeID, RelationID};

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
    pub fn set_attributes(
        &mut self,
        set: *mut AttributeSet,
    ) {
        unsafe {
            AttributeSet_Free(self.attributes);
            self.attributes.write(*set);
        }
    }
}
