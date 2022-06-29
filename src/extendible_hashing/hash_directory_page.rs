use crate::disk_management::buffer_pool::{RawPage, PAGE_SIZE};

/// Hash directory page layout:
/// First four bytes: own page id
/// Second four bytes: log id
/// Next byte: global_depth
/// Next 817 bytes: u8 values of local depths
/// Next 817 * 4 bytes: u32 page_id values for the buckets

/// 512 page_ids can be stored as a result of the page_size, since the buckets must follow 2^n
#[derive(Debug)]
pub struct HashDirectoryPage {
    page_id: u32,
    log_id: u32,
    global_depth: u8,
    local_depths: [u8; 512],
    pub bucket_page_ids: [u32; 512],
}
impl HashDirectoryPage {
    pub fn from_raw_page(raw_page: &RawPage) -> Result<HashDirectoryPage, &str> {
        let bytes = raw_page.data.read().unwrap();
        let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();
        let page_id: u32 = bincode::decode_from_slice(&bytes[0..4], config).unwrap().0;
        let log_id: u32 = bincode::decode_from_slice(&bytes[4..8], config).unwrap().0;
        let global_depth: u8 = bytes[8];

        // 64 bytes of u8 values
        let local_depths: [u8; 512] = bytes[9..521].try_into().unwrap();

        let bucket_page_ids: [u32; 512] = bincode::decode_from_slice(&bytes[521..2569], config)
            .unwrap()
            .0;

        Ok(HashDirectoryPage {
            page_id,
            log_id,
            global_depth,
            local_depths,
            bucket_page_ids,
        })
    }
    pub fn new_empty(
        own_pid: u32,
        bucket1_pid: u32,
        bucket2_pid: u32,
        log_id: u32,
    ) -> HashDirectoryPage {
        let mut local_depths = [0; 512];
        local_depths[0] = 1;
        local_depths[1] = 1;

        let mut bucket_pids = [0; 512];
        bucket_pids[0] = bucket1_pid;
        bucket_pids[1] = bucket2_pid;
        HashDirectoryPage {
            page_id: own_pid,
            log_id,
            global_depth: 1,
            local_depths,
            bucket_page_ids: bucket_pids,
        }
    }
    pub fn to_raw_page(&self) -> RawPage {
        let mut vec = Vec::with_capacity(PAGE_SIZE);
        let bincode_config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();

        vec.append(
            &mut bincode::encode_to_vec(self.page_id, bincode_config)
                .expect("Could not encode the page_id into slice of u8s"),
        );

        vec.append(
            &mut bincode::encode_to_vec(self.log_id, bincode_config)
                .expect("Could not encode the page_id into slice of u8s"),
        );
        vec.push(self.global_depth);
        vec.extend(self.local_depths);
        vec.append(
            &mut bincode::encode_to_vec(self.bucket_page_ids, bincode_config)
                .expect("Could not encode the page_id into slice of u8s"),
        );
        vec.extend(vec![0; PAGE_SIZE - vec.len()]);
        RawPage::new(vec.try_into().unwrap())
    }
    pub fn get_local_depth(&self, index: usize) -> Option<&u8> {
        self.local_depths.get(index)
    }

    pub fn set_local_depth(&mut self, index: usize, local_depth: u8) -> Result<(), &str> {
        if let Some(depth) = self.local_depths.get_mut(index) {
            *depth = local_depth;
            return Ok(());
        }
        Err("Index out of bounds")
    }
    pub fn increment_local_depth(&mut self, index: usize) -> Result<u8, &str> {
        if let Some(depth) = self.local_depths.get_mut(index) {
            *depth += 1;
            return Ok(*depth);
        }
        Err("Index out of bounds")
    }
    pub fn get_bucket_page_id(&self, index: usize) -> Option<&u32> {
        self.bucket_page_ids.get(index)
    }
    pub fn set_bucket_page_id(&mut self, index: usize, page_id: u32) -> Result<(), &str> {
        if let Some(depth) = self.bucket_page_ids.get_mut(index) {
            *depth = page_id;
            return Ok(());
        }
        Err("Index out of bounds")
    }

    pub fn get_global_depth(&self) -> u8 {
        self.global_depth
    }

    pub fn increment_global_depth(&mut self) -> u8 {
        self.global_depth += 1;
        self.global_depth
    }
}
