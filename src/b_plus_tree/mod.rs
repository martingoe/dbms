use bincode::{Decode, Encode};
use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    common::rid::Rid,
    disk_management::buffer_pool::{self, BufferPool, RawPage},
};

use self::{
    b_plus_tree_internal_page::BPlusTreeInternalPage, b_plus_tree_leaf_page::BPlusTreeLeafPage,
};

pub mod b_plus_tree_internal_page;
pub mod b_plus_tree_leaf_page;

enum BPlusTreePage<KeyType: Debug + Eq + Decode + Encode + Ord> {
    InternalPage(BPlusTreeInternalPage<KeyType>),
    LeafPage(BPlusTreeLeafPage<KeyType>),
}

impl<KeyType: Debug + Eq + Decode + Encode + Ord> BPlusTreePage<KeyType> {
    fn from_raw_page(raw_page: &RawPage) -> Option<BPlusTreePage<KeyType>> {
        let data = raw_page.data.read().ok()?;
        let match_thing = data[4];
        drop(data);
        match match_thing {
            1 => Some(BPlusTreePage::LeafPage(BPlusTreeLeafPage::from_raw_page(
                raw_page,
            )?)),
            0 => Some(BPlusTreePage::InternalPage(
                BPlusTreeInternalPage::from_raw_page(&raw_page)?,
            )),
            _ => None,
        }
    }
}
pub struct BPlusTreeIndex<KeyType: Debug + Eq + Decode + Encode + Ord> {
    root_pid: u32,
    phantom: PhantomData<KeyType>,
}

impl<KeyType: Debug + Eq + Decode + Encode + Ord> BPlusTreeIndex<KeyType> {
    pub fn search(&self, key: &KeyType, buffer_pool: Arc<Mutex<BufferPool>>) -> Option<Rid> {
        let current_page = self.get_leaf_of(key, buffer_pool)?;
        match current_page {
            BPlusTreePage::InternalPage(..) => None,
            BPlusTreePage::LeafPage(leaf_page) => leaf_page.get_rid_of(key).copied(),
        }
    }

    pub fn insert(
        &mut self,
        key: KeyType,
        rid: Rid,
        buffer_pool: Arc<Mutex<BufferPool>>,
    ) -> Option<()> {
        let current_page = self.get_leaf_of(&key, buffer_pool.clone())?;
        match current_page {
            BPlusTreePage::InternalPage(..) => None,
            BPlusTreePage::LeafPage(mut leaf_page) => {
                leaf_page.insert(key, rid)?;

                let mut buffer_lock = buffer_pool.lock().ok()?;

                buffer_lock
                    .update_page(leaf_page.get_own_pid() as usize, leaf_page.to_raw_page()?)
                    .ok()?;
                Some(())
            }
        };
        Some(())
    }
    fn get_leaf_of(
        &self,
        key: &KeyType,
        buffer_pool: Arc<Mutex<BufferPool>>,
    ) -> Option<BPlusTreePage<KeyType>> {
        let mut buffer_pool = buffer_pool.lock().ok()?;
        let current_frame = buffer_pool.load_page(self.root_pid as usize)?;
        let mut current_page =
            BPlusTreePage::<KeyType>::from_raw_page(buffer_pool.data[current_frame].as_ref()?)?;
        while let BPlusTreePage::InternalPage(internal_page) = current_page {
            let next_pid = internal_page.get_child_node(key);

            let current_frame = buffer_pool.load_page(next_pid as usize)?;
            current_page = BPlusTreePage::from_raw_page(buffer_pool.data[current_frame].as_ref()?)?;
        }
        Some(current_page)
    }

    fn get_first_leaf(
        &self,
        buffer_pool: Arc<Mutex<BufferPool>>,
    ) -> Option<BPlusTreeLeafPage<KeyType>> {
        let mut buffer_pool = buffer_pool.lock().ok()?;
        let current_frame = buffer_pool.load_page(self.root_pid as usize)?;
        let mut current_page =
            BPlusTreePage::<KeyType>::from_raw_page(buffer_pool.data[current_frame].as_ref()?)?;
        while let BPlusTreePage::InternalPage(internal_page) = current_page {
            let next_pid = internal_page.get_first_child()?;

            let current_frame = buffer_pool.load_page(next_pid as usize)?;
            current_page = BPlusTreePage::from_raw_page(buffer_pool.data[current_frame].as_ref()?)?;
        }
        if let BPlusTreePage::LeafPage(leaf) = current_page {
            Some(leaf)
        } else {
            None
        }
    }
}

// pub struct BPlusTreeIter<KeyType: Eq + Ord + Encode + Decode + Debug> {
//     current_index: usize,
//     current_page: BPlusTreeLeafPage<KeyType>,
//     buffer_pool: Arc<Mutex<BufferPool>>,
// }

// impl<KeyType: 'a + Eq + Ord + Encode + Decode + Debug> Iterator for BPlusTreeIter<KeyType> {
//     type Item<'a> = (&'a KeyType, &'a Rid) where KeyType: 'a;
//     fn next(&mut self) -> Option<Self::Item> {
//         self.current_index += 1;
//         if let Some(key) = self.current_page.get_key_at(self.current_index) {
//             return Some((key, self.current_page.get_rid_at(self.current_index)?));
//         }
//     }
// }
