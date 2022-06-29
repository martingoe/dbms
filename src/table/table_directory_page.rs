use crate::disk_management::buffer_pool::{RawPage, PAGE_SIZE};

// PAGE FORMAT:
//
// ------------------------------------------------------------------------------------------------
// | HEADER | CAPACITY (0) [u8] + PAGE_ID (0) [u32] | ... | CAPACITY (n) [u8] + PAGE_ID (n) [u32] |
// ------------------------------------------------------------------------------------------------
//
// HEADER [16 bytes]:
// --------------------------------------------------------------------------------
// | SELF PAGE_ID [u32] | LSN [u32] | PREV_DIRECTORY [u32] | NEXT_DIRECTORY [u32] |
// --------------------------------------------------------------------------------

#[derive(bincode::Encode, bincode::Decode, Copy, Clone, Debug)]
struct DirectoryEntry {
    capacity: u8,
    page_id: u32,
}

#[derive(bincode::Encode, bincode::Decode, Debug)]
pub struct TableDirectoryPage {
    own_pid: u32,
    lsn: u32,
    prev_directory: u32,
    next_directory: u32,
    entries: [DirectoryEntry; (PAGE_SIZE - 16) / 5],
}

impl TableDirectoryPage {
    pub fn from_raw_page(raw_page: &RawPage) -> Result<TableDirectoryPage, &str> {
        let bincode_config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();

        let data = raw_page.data.read().unwrap();
        let res = bincode::decode_from_slice(&data.as_slice(), bincode_config);
        let res = res.expect("Could not build Table Directory").0;
        Ok(res)
    }
    pub fn to_raw_page(&self) -> RawPage {
        let bincode_config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();
        let mut slice = [0; 4096];
        bincode::encode_into_slice(self, &mut slice, bincode_config)
            .expect("Unexpected error while creating raw page");
        RawPage::new(slice)
    }
}

#[test]
fn from_raw_page_test() {
    let mut raw_page_content = [0; 4096];
    // PID: 12
    [12, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0].swap_with_slice(&mut raw_page_content[0..16]);
    raw_page_content[4095] = 34;

    let raw_page = RawPage::new(raw_page_content);
    let tuple_page = TableDirectoryPage::from_raw_page(&raw_page).expect("expect to build page");
    assert_eq!(tuple_page.own_pid, 12);
    assert_eq!(tuple_page.lsn, 1);
    assert_eq!(tuple_page.prev_directory, 0);

    assert_eq!(tuple_page.next_directory, 1);
}

#[test]
fn to_raw_page() {
    let directory_page = TableDirectoryPage {
        own_pid: 20,
        lsn: 512,
        prev_directory: 124,
        next_directory: 125,
        entries: [DirectoryEntry {
            capacity: 0,
            page_id: 0,
        }; (PAGE_SIZE - 16) / 5],
    };

    let mut expected = [0_u8; PAGE_SIZE];
    [20, 0, 0, 0, 0, 2, 0, 0, 124, 0, 0, 0, 125, 0, 0, 0].swap_with_slice(&mut expected[0..16]);
    let actual = directory_page.to_raw_page();
    let actual_data = actual.data.read().unwrap();
    assert!(actual_data.eq(&expected));
}
