use bincode::{Decode, Encode};

#[derive(Copy, Clone, Debug, PartialEq, Decode, Encode)]
pub struct Rid {
    page_id: u32,
    slot_id: u32,
}
pub const RID_SIZE: usize = 8;
impl Rid {
    pub fn new(page_id: u32, slot_id: u32) -> Rid {
        Rid { page_id, slot_id }
    }
}
