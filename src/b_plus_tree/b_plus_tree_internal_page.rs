use bincode::{Decode, Encode};

use crate::disk_management::buffer_pool::{RawPage, PAGE_SIZE};

#[derive(Encode, Decode)]
struct KeyPagePair<KeyType> {
    key: KeyType,
    page_id: u32,
}

#[derive(Decode, Encode)]
struct BPlusTreeInternalPageHeader {
    own_pid: u32,
    b_plus_tree_page_type: u8,
    lsn: u32,
    current_size: u32,
    max_size: u32,
    parent_pid: u32,
}
/// Header (21 Bytes):
/// --------------------------------------------------------------------------------------------------------
/// | OWN_PID (4) | B_PLUS_TREE_PAGE_TYPE (1) | LSN (4) | CURRENT_SIZE (4) | MAX_SIZE (4) | PARENT_PID (4) |
/// --------------------------------------------------------------------------------------------------------
///
/// Content:
/// ----------------------------------------------------------------------------
/// | HEADER (21) | KEY (k) 1 + PAGE_ID (4) 1 | ... | KEY (k) n + PAGE_ID (4) n|
/// ----------------------------------------------------------------------------

pub struct BPlusTreeInternalPage<KeyType: Ord + Encode + Decode> {
    header: BPlusTreeInternalPageHeader,
    key_page_pairs: Vec<KeyPagePair<KeyType>>,
}
impl<KeyType: Ord + Decode + Encode> BPlusTreeInternalPage<KeyType> {
    pub fn from_raw_page(raw_page: &RawPage) -> Option<BPlusTreeInternalPage<KeyType>> {
        let raw_page_data_lock = raw_page.data.read().unwrap();
        let bincode_config = bincode::config::standard().with_fixed_int_encoding();
        let header: BPlusTreeInternalPageHeader =
            bincode::decode_from_slice(&raw_page_data_lock[0..21], bincode_config)
                .expect("Cannot decode header for b+ tree internal page.")
                .0;

        let mut vec: Vec<KeyPagePair<KeyType>> = Vec::with_capacity(header.current_size as usize);
        let key_size = std::mem::size_of::<KeyType>() + 4;
        for i in 0..header.current_size as usize {
            let start_index = 21 + (i * key_size);
            vec[i] = bincode::decode_from_slice(
                &raw_page_data_lock[start_index..start_index + key_size],
                bincode_config,
            )
            .ok()?
            .0;
        }
        Some(BPlusTreeInternalPage {
            header,
            key_page_pairs: vec,
        })
    }
    pub fn to_raw_page(self) -> Option<RawPage> {
        let mut res: Vec<u8> = vec![0; PAGE_SIZE];

        let bincode_config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();

        let key_size = std::mem::size_of::<KeyType>() + 4;
        let current_size = self.header.current_size;
        bincode::encode_into_slice(self.header, &mut res[0..22], bincode_config).ok()?;
        bincode::encode_into_slice(
            self.key_page_pairs,
            &mut res[21..(21 + key_size * current_size as usize)],
            bincode_config,
        )
        .ok()?;
        res.resize_with(PAGE_SIZE, Default::default);
        Some(RawPage::new(res.try_into().ok()?))
    }
}

#[test]
fn to_raw_page_test() {
    let page = BPlusTreeInternalPage::<u32> {
        header: BPlusTreeInternalPageHeader {
            own_pid: 10,
            b_plus_tree_page_type: 0,
            lsn: 1,
            current_size: 3,
            max_size: 120,
            parent_pid: 0,
        },
        key_page_pairs: vec![
            KeyPagePair::<u32> {
                key: 15,
                page_id: 0,
            },
            KeyPagePair::<u32> {
                key: 20,
                page_id: 20,
            },
            KeyPagePair::<u32> {
                key: 45,
                page_id: 21,
            },
        ],
    };
    todo!();
}
