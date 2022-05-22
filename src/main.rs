use crate::disk_management::{buffer_pool::{BufferPool, RawPage, PAGE_SIZE}, disk_manager::DiskManager};

mod disk_management;

fn main() {
    let mut file_manager = DiskManager::new("resources/db_save_files/test.mdb".to_string());
    let mut buffer_pool = BufferPool::new(&mut file_manager);
    let frame = buffer_pool.load_page(0);
    println!("Hello, world! {:?}, {:?}", frame, buffer_pool.data[frame]);
}
