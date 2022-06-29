#![feature(assert_matches)]
use std::sync::{Arc, Mutex};

use rand::Rng;

use crate::{
    disk_management::{buffer_pool::BufferPool, disk_manager::DiskManager},
    extendible_hashing::extendible_hashing::ExtendibleHashing,
};

pub mod disk_management;
mod extendible_hashing;

fn main() {
    let file_manager = Arc::new(Mutex::new(DiskManager::new(
        "resources/db_save_files/test.mdb".to_string(),
    )));
    let buffer_pool = BufferPool::new(file_manager);
    let buffer_pool_mutex = Arc::new(Mutex::new(buffer_pool));

    let extendible_hashing =
        ExtendibleHashing::<u32, u32>::setup_new_hashmap(buffer_pool_mutex.clone(), 2)
            .expect("Could not create hashmap");
    println!(
        "directory page id: {:?}",
        extendible_hashing.directory_page_id
    );
    // let directory_page = HashDirectoryPage::from_raw_page(lock.get_raw_page(frame).unwrap());
    // println!("{:?}", directory_page);

    // let frame2 = lock.load_page(2).unwrap();
    // let bucket_page = HashBucketPage::<u32, u32>::from_raw_page(lock.get_raw_page(frame2).unwrap());
    // println!("{:?}", bucket_page);
    // .expect("Could not create new hashmap");
    let mut rng = rand::thread_rng();
    for _ in 0..10_000 {
        extendible_hashing.insert(rng.gen(), rng.gen());
    }

    buffer_pool_mutex
        .lock()
        .unwrap()
        .unload_all_pages_and_write_to_file();
}
