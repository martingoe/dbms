use bincode::{Decode, Encode};
use std::fmt::Debug;

use crate::{
    common::rid::{Rid, RID_SIZE},
    disk_management::buffer_pool::{RawPage, PAGE_SIZE},
};

#[derive(Decode, Encode)]
pub struct BPlusTreeLeafPageHeader {
    own_pid: u32,
    b_plus_tree_page_type: u8,
    lsn: u32,
    current_size: u32,
    max_size: u32,
    parent_pid: u32,
    next_leaf: u32,
    prev_leaf: u32,
}
/// Header (29 Bytes):
/// ----------------------------------------------------------------------------------------------------------------------------------------
/// | OWN_PID (4) | B_PLUS_TREE_PAGE_TYPE (1) | LSN (4) | CURRENT_SIZE (4) | MAX_SIZE (4) | PARENT_PID (4) | NEXT_LEAF (4) | PREV_LEAF (4) |
/// ----------------------------------------------------------------------------------------------------------------------------------------
///
/// Content:
/// -----------------------------------------------------------------------------------
/// | HEADER (29) | KEY (k) 1 | ... | KEY (k) n | RID (4) 1 | ... | RID (8) n |
/// -----------------------------------------------------------------------------------

pub struct BPlusTreeLeafPage<KeyType: Ord + Encode + Decode + Debug + Eq> {
    header: BPlusTreeLeafPageHeader,
    keys: Vec<KeyType>,
    rids: Vec<Rid>,
}

impl<KeyType: Ord + Encode + Decode + Debug + Eq> BPlusTreeLeafPage<KeyType> {
    pub fn from_raw_page(raw_page: &RawPage) -> Option<BPlusTreeLeafPage<KeyType>> {
        let bincode_conf = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();
        let data = raw_page.data.read().ok()?;
        let header: BPlusTreeLeafPageHeader =
            bincode::decode_from_slice(&data[0..29], bincode_conf)
                .ok()?
                .0;

        let key_size = std::mem::size_of::<KeyType>();

        let mut key_start = 29;
        let mut rid_start = PAGE_SIZE - (header.max_size as usize * RID_SIZE);
        let mut keys: Vec<KeyType> = Vec::with_capacity(header.max_size as usize);

        let mut rids: Vec<Rid> = Vec::with_capacity(header.max_size as usize);
        for _ in 0..header.current_size {
            keys.push(
                bincode::decode_from_slice(&data[key_start..key_start + key_size], bincode_conf)
                    .ok()?
                    .0,
            );

            rids.push(
                bincode::decode_from_slice(&data[rid_start..rid_start + key_size], bincode_conf)
                    .ok()?
                    .0,
            );
            key_start += key_size;
            rid_start += RID_SIZE;
        }
        Some(BPlusTreeLeafPage { header, keys, rids })
    }

    pub fn to_raw_page(mut self) -> Option<RawPage> {
        let bincode_config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();

        let current_size = self.header.current_size as usize;
        let max_size = self.header.max_size as usize;
        let mut result_vec = vec![0; PAGE_SIZE];
        bincode::encode_into_slice(self.header, &mut result_vec[0..29], bincode_config).ok()?;

        let key_size = std::mem::size_of::<KeyType>();

        let mut key_start = 29;
        let mut rid_start = PAGE_SIZE - (max_size * RID_SIZE);
        for i in self.keys.drain(0..current_size) {
            bincode::encode_into_slice(
                i,
                &mut result_vec[key_start..key_start + key_size],
                bincode_config,
            )
            .ok()?;
            key_start += key_size;
        }

        for i in self.rids.drain(0..current_size) {
            bincode::encode_into_slice(
                i,
                &mut result_vec[rid_start..rid_start + RID_SIZE],
                bincode_config,
            )
            .ok()?;
            rid_start += RID_SIZE;
        }
        Some(RawPage::new(result_vec.try_into().ok()?))
    }

    fn get_next_leaf(&self) -> u32 {
        self.header.next_leaf
    }

    fn get_previous_leaf(&self) -> u32 {
        self.header.prev_leaf
    }
    pub fn get_rid_of(&self, key: &KeyType) -> Option<&Rid> {
        let index = self.keys.binary_search(key).ok()?;
        self.rids.get(index)
    }
    pub fn get_key_at(&self, index: usize) -> Option<&KeyType> {
        self.keys.get(index)
    }

    pub fn get_rid_at(&self, index: usize) -> Option<&Rid> {
        self.rids.get(index)
    }

    pub fn insert(&mut self, key: KeyType, rid: Rid) -> Option<usize> {
        if self.header.current_size == self.header.max_size {
            todo!("Filled node")
        }

        let pos = match self.keys.binary_search(&key) {
            Ok(pos) => pos, // element already in vector @ `pos`
            Err(pos) => pos,
        };

        self.keys.insert(pos, key);
        self.rids.insert(pos, rid);
        Some(pos)
    }
    pub fn remove(&mut self, key: &KeyType) -> Option<(KeyType, Rid)> {
        if self.header.current_size == self.header.max_size / 2 {
            todo!("Cannot remove object from half-filled node.")
        }

        let pos = match self.keys.binary_search(key) {
            Ok(pos) => pos, // element already in vector @ `pos`
            Err(pos) => pos,
        };
        let key = self.keys.remove(pos);
        let rid = self.rids.remove(pos);
        Some((key, rid))
    }
    pub fn get_own_pid(&self) -> u32 {
        self.header.own_pid
    }
}
