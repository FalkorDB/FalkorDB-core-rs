use crate::attribute_set::ffi::AttributeSet_Free;
use crate::attribute_set::AttributeSet;
use crate::types::EntityID;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Node {
    pub attributes: *mut AttributeSet,
    pub id: EntityID,
}

impl Node {
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
