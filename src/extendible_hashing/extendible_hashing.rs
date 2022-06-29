use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
};

use bincode::{Decode, Encode};

use crate::disk_management::buffer_pool::{BufferPool, RawPage};

use super::{hash_bucket_page::HashBucketPage, hash_directory_page::HashDirectoryPage};
use std::fmt::Debug;

pub struct ExtendibleHashing<
    K: Hash + Clone + Debug + Encode + Decode + Eq + Default,
    V: Clone + Debug + Encode + Decode + Default,
> {
    buffer_pool: Arc<Mutex<BufferPool>>,
    pub directory_page_id: u32,
    phantom_data: PhantomData<(K, V)>,
}
impl<
        K: Hash + Clone + Debug + Encode + Decode + Eq + Default,
        V: Clone + Debug + Encode + Decode + Default,
    > ExtendibleHashing<K, V>
{
    pub fn new(
        buffer_pool: Arc<Mutex<BufferPool>>,
        directory_page_id: u32,
    ) -> ExtendibleHashing<K, V> {
        ExtendibleHashing {
            buffer_pool,
            directory_page_id,
            phantom_data: PhantomData,
        }
    }
    pub fn setup_new_hashmap(
        buffer_pool: Arc<Mutex<BufferPool>>,
        log_id: u32,
    ) -> Result<ExtendibleHashing<K, V>, &'static str> {
        let mut buffer_pool_lock = buffer_pool.lock().expect("could not lock buffer_pool");
        let (directory_page_id, directory_frame_id) = buffer_pool_lock
            .load_new_page()
            .expect("Could not load a new page");

        let bucket1_pid = buffer_pool_lock.allocate_new_page();
        let bucket2_pid = buffer_pool_lock.allocate_new_page();
        let directory_page = HashDirectoryPage::new_empty(
            directory_page_id as u32,
            bucket1_pid as u32,
            bucket2_pid as u32,
            log_id,
        );
        buffer_pool_lock
            .update_page(directory_page_id, directory_page.to_raw_page())
            .expect("Could not update directory page");

        buffer_pool_lock.unload_page_id(directory_page_id).unwrap();

        Ok(ExtendibleHashing {
            buffer_pool: buffer_pool.clone(),
            directory_page_id: directory_page_id as u32,
            phantom_data: PhantomData,
        })
    }

    fn bucket_index_of_key(key: &K, directory_page: &HashDirectoryPage) -> u64 {
        let hash = get_hash(key);
        let bucket = hash % (1 << directory_page.get_global_depth());
        bucket
    }

    pub fn insert(&self, key: K, value: V) {
        let mut buffer_pool_lock = self.buffer_pool.lock().expect("Could not lock buffer pool");

        self.insert_with_lock(&mut buffer_pool_lock, key, value);
    }

    fn insert_with_lock(&self, buffer_pool_lock: &mut MutexGuard<BufferPool>, key: K, value: V) {
        let directory_frame_id = buffer_pool_lock
            .load_page(self.directory_page_id as usize)
            .expect("Could not load the directory page");
        let mut directory_page = HashDirectoryPage::from_raw_page(
            buffer_pool_lock.get_raw_page(directory_frame_id).unwrap(),
        )
        .expect("Could not create a directory page from the raw page.");
        let bucket_index = ExtendibleHashing::<K, V>::bucket_index_of_key(&key, &directory_page);
        let bucket_page_id = *directory_page
            .get_bucket_page_id(bucket_index as usize)
            .unwrap() as usize;
        let bucket_frame_id = buffer_pool_lock
            .load_page(bucket_page_id)
            .expect("Could not load the bucket page");
        let mut bucket_page = HashBucketPage::<K, V>::from_raw_page(
            buffer_pool_lock.get_raw_page(bucket_frame_id).unwrap(),
        );

        if bucket_page.is_full() {
            self.split_bucket(
                bucket_index as usize,
                &mut bucket_page,
                &mut directory_page,
                buffer_pool_lock,
            )
            .expect("Could not split bucket");
            self.update_directory_and_bucket(
                buffer_pool_lock,
                directory_page.to_raw_page(),
                bucket_page_id,
                bucket_page.to_raw_page(),
            );
            self.insert_with_lock(buffer_pool_lock, key, value);
        } else {
            bucket_page
                .insert(key, value)
                .expect("Could not insert into the bucket page that wasn't supposed to be full.");
        }
        self.update_directory_and_bucket(
            buffer_pool_lock,
            directory_page.to_raw_page(),
            bucket_page_id,
            bucket_page.to_raw_page(),
        );
    }

    fn update_directory_and_bucket(
        &self,
        buffer_pool_lock: &mut MutexGuard<BufferPool>,
        directory_page: RawPage,
        bucket_page_id: usize,
        bucket_page: RawPage,
    ) {
        buffer_pool_lock
            .update_page(self.directory_page_id as usize, directory_page)
            .unwrap();
        buffer_pool_lock
            .unload_page_id(self.directory_page_id as usize)
            .expect("Could not unload");
        buffer_pool_lock
            .update_page(bucket_page_id, bucket_page)
            .expect("Could not update bucket page.");
        buffer_pool_lock
            .unload_page_id(bucket_page_id)
            .expect("Could not unload");
    }

    fn split_bucket(
        &self,
        bucket_index: usize,
        bucket_page: &mut HashBucketPage<K, V>,
        directory_page: &mut HashDirectoryPage,
        buffer_pool_lock: &mut MutexGuard<BufferPool>,
    ) -> Result<(), &str> {
        let new_local_depth = directory_page.increment_local_depth(bucket_index).unwrap();
        let (new_bucket_page_id, new_bucket_page_frame_id) =
            buffer_pool_lock.load_new_page().unwrap();

        let mut new_bucket_page = HashBucketPage::<K, V>::from_raw_page(
            buffer_pool_lock
                .get_raw_page(new_bucket_page_frame_id)
                .unwrap(),
        );
        let old_bucket_page_id = *directory_page.get_bucket_page_id(bucket_index).unwrap();

        // Let old bucket be with 1 in front, new with 0.
        if directory_page.get_global_depth() < new_local_depth {
            // Global Split
            global_split_bucket(directory_page, bucket_index, new_bucket_page_id);
        } else {
            local_split_bucket(
                directory_page,
                new_local_depth,
                new_bucket_page_id,
                old_bucket_page_id,
            );
        }

        for i in 0..bucket_page.key_values.len() {
            let key = bucket_page.key_at(i).unwrap();
            if (get_hash(key) >> (new_local_depth - 1)) & 1 == 0 {
                let key_value = bucket_page.remove_index(i).unwrap();
                new_bucket_page
                    .insert(key_value.0, key_value.1)
                    .expect("Could not insert the value into the new bucket.");
            }
        }

        buffer_pool_lock
            .update_page(new_bucket_page_id, new_bucket_page.to_raw_page())
            .expect("Could not update new page");
        buffer_pool_lock
            .unload_page_id(new_bucket_page_id)
            .expect("Could not unload new bucket page");

        Ok(())
    }

    fn remove(&self, key: K) -> Option<(K, V)> {
        let mut lock = self
            .buffer_pool
            .lock()
            .expect("Could not lock the buffer pool.");

        let directory_frame = lock
            .load_page(self.directory_page_id as usize)
            .expect("Could not load directory page");

        let directory_page = HashDirectoryPage::from_raw_page(
            lock.get_raw_page(directory_frame)
                .expect("Could not load previously loaded frame"),
        )
        .expect("Could not parse directory page from raw page");

        let index = ExtendibleHashing::<K, V>::bucket_index_of_key(&key, &directory_page);

        let bucket_pid = (*directory_page.get_bucket_page_id(index as usize).unwrap()) as usize;
        let bucket_frame = lock.load_page(bucket_pid)?;

        let mut bucket_page =
            HashBucketPage::<K, V>::from_raw_page(lock.get_raw_page(bucket_frame).unwrap());
        let result = bucket_page.remove(&key).ok();
        let raw_page = bucket_page.to_raw_page();
        self.update_directory_and_bucket(
            &mut lock,
            directory_page.to_raw_page(),
            bucket_pid,
            raw_page,
        );
        result
    }
}

fn local_split_bucket(
    directory_page: &mut HashDirectoryPage,
    new_local_depth: u8,
    new_bucket_page_id: usize,
    old_bucket_page_id: u32,
) {
    // Change all bucket pointers that point to the old bucket and have a preceding 1 to the new bucket.
    for i in 0..(1 << directory_page.get_global_depth()) {
        if *directory_page.get_bucket_page_id(i).unwrap() == old_bucket_page_id {
            directory_page.set_local_depth(i, new_local_depth).unwrap();

            if i >> (new_local_depth - 1) & 1 == 0 {
                directory_page
                    .set_bucket_page_id(i, new_bucket_page_id as u32)
                    .unwrap();
            }
        }
    }
}

fn global_split_bucket(
    directory_page: &mut HashDirectoryPage,
    bucket_index: usize,
    new_bucket_page_id: usize,
) {
    let old_global_depth = directory_page.get_global_depth();
    directory_page.increment_global_depth();
    // TODO: Test for largest possible global depth == 9
    for i in 0..(1 << old_global_depth) {
        directory_page
            .set_bucket_page_id(
                i + (1 << old_global_depth),
                *directory_page.get_bucket_page_id(i).unwrap(),
            )
            .unwrap();

        directory_page
            .set_local_depth(
                i + (1 << old_global_depth),
                *directory_page.get_local_depth(i).unwrap(),
            )
            .unwrap();
    }
    directory_page
        .set_bucket_page_id(bucket_index, new_bucket_page_id as u32)
        .expect("Could not set the bucket page id");
}

fn get_hash<K: Hash>(key: K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}
