use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use super::{disk_manager::DiskManager, lru_replacer::LRUReplacer};

pub const PAGE_SIZE: usize = 4096;
const POOL_SIZE: usize = 100;

pub struct ParallelBufferPoolWrapper<'a> {
    pub buffer_pool: Mutex<BufferPool<'a>>,
}

pub struct BufferPool<'a> {
    pub data: Vec<Option<RawPage>>,
    pub page_table: HashMap<usize, PageTableEntry>,
    lru_replacer: LRUReplacer,
    file_manager: &'a mut DiskManager,
}

impl BufferPool<'_> {
    pub fn new(file_manager: &mut DiskManager) -> BufferPool {
        let vec: Vec<Option<RawPage>> = vec![None; POOL_SIZE];
        return BufferPool {
            data: vec,
            page_table: HashMap::new(),
            lru_replacer: LRUReplacer::new(POOL_SIZE),
            file_manager,
        };
    }

    pub fn load_page(&mut self, page_id: usize) -> Option<usize> {
        let possible_page_table = self.page_table.get_mut(&page_id);
        if let Some(page_table) = possible_page_table {
            page_table.ref_count += 1;
            if page_table.ref_count == 1 {
                self.lru_replacer.drop_page(page_id);
            }
            return Some(page_table.frame_index);
        }

        // No free frame, evicting page is necessary
        if self.page_table.len() == POOL_SIZE {
            let index_to_remove = self.lru_replacer.pop_least_recently_used();
            if let Some(index) = index_to_remove {
                let page_table_entry = self
                    .page_table
                    .get(&index)
                    .expect("Could not find the page table entry");
                let frame_index = page_table_entry.frame_index;
                if page_table_entry.dirty {
                    self.file_manager.write_page(
                        index,
                        &self.data[frame_index]
                            .as_ref()
                            .expect("Expected a filled page that isn't filled"),
                    );
                }

                return Some(self.load_page_from_disk(page_id, frame_index));
            }
            return None;
        }

        let frame_index = self
            .data
            .iter()
            .enumerate()
            .filter(|(_, value)| value.is_none())
            .next()
            .expect("could not find a none-value")
            .0;

        return Some(self.load_page_from_disk(page_id, frame_index));
    }

    pub fn unload_page_id(&mut self, page_id: usize) -> Result<(), &str> {
        let mut page_entry = self
            .page_table
            .get_mut(&page_id)
            .ok_or("Cannot find the specified page index")?;
        if page_entry.ref_count == 0 {
            return Err("There is currently no reference to the specified page");
        }
        page_entry.ref_count -= 1;
        if page_entry.ref_count == 0 {
            self.lru_replacer.add_page(page_id);
        }
        return Ok(());
    }

    pub fn unload_all_pages_and_write_to_file(&mut self) {
        for (page_id, page_table) in self.page_table.drain() {
            if page_table.dirty {
                self.file_manager.write_page(
                    page_id,
                    &self
                        .data
                        .get(page_table.frame_index)
                        .expect("The loaded frame index is out of bounds")
                        .as_ref()
                        .expect("The frame was not loaded"),
                );
            }
        }

        self.data.fill(None);
        self.lru_replacer.drop_all_pages();
    }

    fn load_page_from_disk(&mut self, page_id: usize, frame_index: usize) -> usize {
        let new_data = self.file_manager.read_page(page_id);
        let raw_page = RawPage::new(new_data);
        self.page_table
            .insert(page_id, PageTableEntry::new(frame_index));
        self.data[frame_index] = Some(raw_page);
        return frame_index;
    }
}

#[derive(Clone, Debug)]
pub struct RawPage {
    pub data: Arc<RwLock<[u8; PAGE_SIZE]>>,
}
impl RawPage {
    pub fn new(data: [u8; PAGE_SIZE]) -> RawPage {
        return RawPage {
            data: Arc::new(RwLock::new(data)),
        };
    }
}

pub struct PageTableEntry {
    pub frame_index: usize,
    dirty: bool,
    ref_count: usize,
}

impl PageTableEntry {
    pub fn new(frame_id: usize) -> PageTableEntry {
        return PageTableEntry {
            frame_index: frame_id,
            dirty: false,
            ref_count: 1,
        };
    }
}
