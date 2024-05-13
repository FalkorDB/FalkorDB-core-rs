use super::AttributeSet;

extern "C" {
    pub fn AttributeSet_Free(set: *mut AttributeSet);
}
