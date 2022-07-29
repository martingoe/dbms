use bincode::{Decode, Encode};

use crate::{
    common::rid::Rid,
    disk_management::buffer_pool::{RawPage, PAGE_SIZE},
};

// | HEADER | ... FREE SPACE ... | TUPLE (n) | ... | TUPLE (1) |
// HEADER:
// | OWN_PID [u32] | FREE_SPACE_POINTER [u16] | TUPLE_COUNT [u16] | TUPLE_OFFSET (1) [u16] | TUPLE_SIZE (1) [u16] |
#[derive(Encode, Decode, Debug)]
struct TupleHeader {
    tuple_offset: u16,
    tuple_size: u16,
    free: bool,
}
impl TupleHeader {
    fn new_occupied(tuple_offset: u16, tuple_size: u16) -> TupleHeader {
        TupleHeader {
            tuple_offset,
            tuple_size,
            free: false,
        }
    }
}
const TUPLE_HEADER_SIZE: u16 = 5;
#[derive(Debug, PartialEq)]
struct Tuple {
    data: Vec<u8>,
    own_rid: Rid,
}

#[derive(Debug)]
pub struct TablePage {
    own_pid: u32,
    free_space_pointer: u16,
    tuple_count: u16,
    tuple_headers: Vec<TupleHeader>,
    tuples: Vec<Tuple>,
}

impl TablePage {
    pub fn from_raw_page(raw_page: &RawPage) -> Result<TablePage, &str> {
        let data = raw_page.data.read().unwrap();
        let config = bincode::config::standard().with_fixed_int_encoding();
        let own_pid: u32 = bincode::decode_from_slice(&data[0..4], config)
            .or(Err("Malformed raw page"))?
            .0;

        let free_space_pointer: u16 = bincode::decode_from_slice(&data[4..6], config)
            .or(Err("Malformed raw page"))?
            .0;

        let tuple_count: u16 = bincode::decode_from_slice(&data[6..8], config)
            .or(Err("Malformed raw page"))?
            .0;

        let mut i = 8;
        let mut tuple_headers = Vec::new();
        let mut tuples = Vec::new();
        for slot_id in 0..tuple_count {
            let tuple_header: TupleHeader =
                bincode::decode_from_slice(&data[i..i + TUPLE_HEADER_SIZE as usize], config)
                    .unwrap()
                    .0;
            println!("{:?}", tuple_header);

            let tuple_data = data[tuple_header.tuple_offset as usize
                ..(tuple_header.tuple_offset as usize + tuple_header.tuple_size as usize)]
                .to_vec();
            tuples.push(Tuple {
                data: tuple_data,
                own_rid: Rid::new(own_pid, slot_id as u32),
            });

            tuple_headers.push(tuple_header);
            i += TUPLE_HEADER_SIZE as usize;
        }
        Ok(TablePage {
            own_pid,
            free_space_pointer,
            tuple_count,
            tuple_headers,
            tuples,
        })
    }

    /// Converts the data to a raw page, possibly to be saved again.
    pub fn to_raw_page(&self) -> RawPage {
        let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .skip_fixed_array_length();
        let mut result_data = [0; PAGE_SIZE];
        bincode::encode_into_slice(self.own_pid, &mut result_data[0..4], config).unwrap();
        bincode::encode_into_slice(self.free_space_pointer, &mut result_data[4..6], config)
            .unwrap();

        bincode::encode_into_slice(self.tuple_count, &mut result_data[6..8], config).unwrap();
        let mut index = 8;
        for i in 0..self.tuple_headers.len() {
            let tuple_header = &self.tuple_headers[i];
            bincode::encode_into_slice(
                &self.tuple_headers[i],
                &mut result_data[index..index + TUPLE_HEADER_SIZE as usize],
                config,
            )
            .unwrap();
            bincode::encode_into_slice(
                &self.tuples[i].data,
                &mut result_data[tuple_header.tuple_offset as usize
                    ..(tuple_header.tuple_offset + tuple_header.tuple_size) as usize],
                config,
            )
            .unwrap();
            index += TUPLE_HEADER_SIZE as usize;
        }
        RawPage::new(result_data)
    }

    /// Inserts data into the table page and returns the Rid of the value.
    pub fn insert(&mut self, tuple_data: Vec<u8>) -> Option<Rid> {
        self.free_space_pointer -= tuple_data.len() as u16;
        if (self.tuple_count + 1) * TUPLE_HEADER_SIZE >= self.free_space_pointer {
            return None;
        }
        let tuple_header = TupleHeader {
            tuple_offset: self.free_space_pointer,
            tuple_size: tuple_data.len() as u16,
            free: false,
        };

        let rid = Rid::new(self.own_pid, self.tuple_headers.len() as u32);
        self.tuple_headers.push(tuple_header);

        self.tuples.push(Tuple {
            data: tuple_data,
            own_rid: rid.clone(),
        });
        self.tuple_count += 1;
        Some(rid)
    }

    pub fn remove(&mut self, slot_id: usize) -> Option<Tuple> {
        if (self.tuple_count as usize) <= slot_id || self.tuple_headers[slot_id].free == true {
            println!("{:?}", self.tuple_headers[slot_id].free);
            return None;
        }
        self.tuple_headers[slot_id].free = true;
        let previous = std::mem::replace(
            &mut self.tuples[slot_id],
            Tuple {
                data: vec![],
                own_rid: Rid::new(self.own_pid, slot_id as u32),
            },
        );

        Some(previous)
    }
}

#[test]
fn from_raw_page_test() {
    let mut raw_page_content = [0; 4096];
    // PID: 12, FREE_SPACE_POINTER: 4093, TUPLE_COUNT: 1, TUPLE_OFFSET 1: 4093, TUPLE_SIZE: 3
    [12, 0, 0, 0, 253, 15, 1, 0, 253, 15, 3, 0, 0].swap_with_slice(&mut raw_page_content[0..13]);
    raw_page_content[4095] = 34;

    let raw_page = RawPage::new(raw_page_content);
    let tuple_page = TablePage::from_raw_page(&raw_page).expect("expect to build page");
    println!("{:?}", tuple_page);
    assert_eq!(tuple_page.free_space_pointer, 4093);
    assert_eq!(tuple_page.own_pid, 12);
    assert_eq!(tuple_page.tuple_count, 1);
    assert_eq!(tuple_page.tuple_headers.len(), 1);
    assert_eq!(tuple_page.tuple_headers[0].tuple_offset, 4093);
    assert_eq!(tuple_page.tuple_headers[0].tuple_size, 3);
    assert_eq!(tuple_page.tuples[0].data[2], 34);
}

#[test]
fn test_insert() {
    let mut table_page = TablePage {
        own_pid: 0,
        free_space_pointer: 4096,
        tuple_count: 0,
        tuple_headers: Vec::new(),
        tuples: Vec::new(),
    };
    table_page.insert(vec![10, 0, 15, 5]);
    assert_eq!(table_page.free_space_pointer, 4092);
    assert_eq!(table_page.tuple_headers.len(), 1);
    assert_eq!(table_page.tuple_headers[0].tuple_offset, 4092);
    assert_eq!(table_page.tuple_headers[0].tuple_size, 4);

    table_page.insert(vec![0, 15, 5]);
    assert_eq!(table_page.free_space_pointer, 4089);
    assert_eq!(table_page.tuple_headers.len(), 2);
    assert_eq!(table_page.tuple_headers[1].tuple_offset, 4089);
    assert_eq!(table_page.tuple_headers[1].tuple_size, 3);
}

#[test]
fn test_remove() {
    let mut table_page = TablePage {
        own_pid: 0,
        free_space_pointer: 4094,
        tuple_count: 1,
        tuple_headers: vec![TupleHeader::new_occupied(4096, 2)],
        tuples: vec![Tuple {
            data: vec![0, 1],
            own_rid: Rid::new(0, 0),
        }],
    };
    println!("{:?}", table_page);

    let old_table = table_page.remove(0);
    let expected = Some(Tuple {
        data: vec![0, 1],
        own_rid: Rid::new(0, 0),
    });
    assert_eq!(old_table, expected);
    assert!(table_page.remove(0).is_none());
}
