use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, RwLock},
};

use super::buffer_pool::{PAGE_SIZE, RawPage};

pub struct DiskManager {
    db_file_path: String,
    file: File,
}

impl DiskManager {
    pub fn new(db_file_path: String) -> DiskManager {
        if !std::path::Path::new(&db_file_path).exists(){
            File::create(db_file_path.to_owned()).expect("Could not create the database file that did not exist");
        }
        let file = File::options()
            .write(true)
            .read(true)
            .open(db_file_path.to_owned()).expect("Could not open the database file");
        return DiskManager { db_file_path, file };
    }

    pub fn write_page(&mut self, page_id: usize, data: &RawPage) {
        self.file
            .seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        self.file
            .write(
                &*data.data
                    .read()
                    .expect("Could not get the value behind the RwLock"),
            )
            .unwrap();
        self.file.flush().expect("Could not flush the page content");
    }

    pub fn read_page(&mut self, page_id: usize) -> [u8; PAGE_SIZE] {
        let mut buffer = [0; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64))
            .unwrap();
        self.file
            .read_exact(&mut buffer)
            .expect("Could not read page contents");
        return buffer;
    }
}
