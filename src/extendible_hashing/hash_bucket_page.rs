use bincode::{Decode, Encode};

use crate::disk_management::buffer_pool::{RawPage, PAGE_SIZE};
use std::fmt::Debug;
#[derive(Debug)]
pub struct HashBucketPage<
    K: Clone + Debug + Encode + Decode + Default,
    V: Clone + Debug + Encode + Decode + Default,
> {
    readable: Vec<bool>,
    has_been_occupied: Vec<bool>,
    pub key_values: Vec<(K, V)>,
}
impl<
        K: Clone + Eq + Debug + Encode + Decode + Default,
        V: Clone + Debug + Encode + Decode + Default,
    > HashBucketPage<K, V>
{
    pub fn from_raw_page(raw_page: &RawPage) -> HashBucketPage<K, V> {
        let data = raw_page
            .data
            .read()
            .expect("Could not read the raw_page data");
        let key_length = std::mem::size_of::<K>();
        let value_length = std::mem::size_of::<V>();
        let length_of_single_entry = 1 + 1 + key_length as usize + value_length as usize;
        let number_of_entries = (PAGE_SIZE) / length_of_single_entry;

        let mut readable = Vec::with_capacity(number_of_entries);
        let mut has_been_occupied = Vec::with_capacity(number_of_entries);

        let mut key_values = Vec::with_capacity(number_of_entries);
        for i in 0..number_of_entries {
            readable.push(data[i] != 0);
            has_been_occupied.push(data[i + number_of_entries] != 0);

            let starting_index = (key_length + value_length) as usize * i + number_of_entries * 2;
            let key_value: (K, V) = bincode::decode_from_slice(
                &data[starting_index..starting_index + (key_length + value_length) as usize],
                bincode::config::standard().with_fixed_int_encoding(),
            )
            .expect("Could not decode key and value from slice.")
            .0;
            key_values.push(key_value);
        }

        HashBucketPage {
            readable,
            has_been_occupied,
            key_values,
        }
    }
    pub fn toggle_readable(&mut self, index: usize) -> Result<(), &str> {
        let element = self.readable.get_mut(index);
        if let Some(some_elem) = element {
            // Toggle the bit
            *some_elem = !*some_elem;
            return Ok(());
        }
        Err("The index is out of bounds.")
    }

    pub fn is_readable(&self, index: usize) -> Option<&bool> {
        self.readable.get(index)
    }

    pub fn toggle_occupied(&mut self, index: usize) -> Result<(), &str> {
        let element = self.has_been_occupied.get_mut(index);
        if let Some(some_elem) = element {
            // Toggle the bit
            *some_elem ^= true;
            return Ok(());
        }
        Err("The index is out of bounds.")
    }

    pub fn has_been_occupied(&self, index: usize) -> Option<&bool> {
        self.has_been_occupied.get(index)
    }

    pub fn set_has_been_occupied(
        &mut self,
        index: usize,
        has_been_occupied: bool,
    ) -> Result<(), &str> {
        let element = self.has_been_occupied.get_mut(index);
        if let Some(some_elem) = element {
            // Toggle the bit
            *some_elem = has_been_occupied;
            return Ok(());
        }
        Err("The index is out of bounds.")
    }

    pub fn is_full(&self) -> bool {
        self.readable.iter().all(|is_readable| *is_readable == true)
    }

    pub fn is_empty(&self) -> bool {
        self.readable
            .iter()
            .all(|is_readable| *is_readable == false)
    }

    fn first_free_index(&self) -> Option<usize> {
        for i in 0..self.readable.len() {
            if !self.readable[i] {
                return Some(i);
            }
        }
        None
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), &str> {
        let index = self
            .first_free_index()
            .ok_or("Inserting would overflow the bucket.")?;

        self.toggle_readable(index).expect("Unreachable");

        self.set_has_been_occupied(index, true)
            .expect("Unreachable");

        if let Some(key_value_pair) = self.key_values.get_mut(index) {
            *key_value_pair = (key, value);
            return Ok(());
        }
        Err("Could not insert the values")
    }

    pub fn remove_index(&mut self, index: usize) -> Result<(K, V), &str> {
        self.toggle_readable(index).expect("Removal out of bounds");
        self.key_values
            .splice(index..index + 1, [Default::default()])
            .next()
            .ok_or("Could not replace the old value with defaults")
    }

    pub fn remove(&mut self, key_to_remove: &K) -> Result<(K, V), &str> {
        let index = self
            .key_values
            .iter()
            .enumerate()
            .filter(|(i, (key, _))| {
                key == key_to_remove && *self.is_readable(*i).expect("Unreachable")
            })
            .next()
            .and_then(|(i, _)| Some(i));

        if let Some(index_to_remove) = index {
            self.toggle_readable(index_to_remove).expect("Unreachable");
            return self
                .key_values
                .splice(index_to_remove..index_to_remove + 1, [Default::default()])
                .next()
                .ok_or("Could not replace the old value with defaults");
        }

        Err("The requested key does not exist.")
    }

    pub fn key_at(&self, index: usize) -> Option<&K> {
        self.key_values.get(index).and_then(|key| Some(&key.0))
    }

    pub fn value_at(&self, index: usize) -> Option<&V> {
        self.key_values.get(index).and_then(|key| Some(&key.1))
    }

    pub fn to_raw_page(&self) -> RawPage {
        let mut data = Vec::with_capacity(PAGE_SIZE);

        for readable in &self.readable {
            if *readable {
                data.push(1);
            } else {
                data.push(0);
            }
        }

        for is_occupied in &self.has_been_occupied {
            if *is_occupied {
                data.push(1);
            } else {
                data.push(0);
            }
        }
        for key_value in &self.key_values {
            data.append(
                &mut bincode::encode_to_vec(
                    &key_value,
                    bincode::config::standard().with_fixed_int_encoding(),
                )
                .expect("Could not encode value to binary"),
            );
        }

        data.append(&mut vec![0; PAGE_SIZE - data.len()]);
        RawPage::new(data.try_into().expect(""))
    }
}
