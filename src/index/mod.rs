#[repr(C)]
#[derive(Copy, Clone)]
pub enum IndexFieldType {
    Unknown = 0x00,
    Fulltext = 0x01,
    Numeric = 0x02,
    Geo = 0x04,
    String = 0x08,
    Vector = 0x10,
}
