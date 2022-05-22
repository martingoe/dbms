use std::cmp::Reverse;

use priority_queue::PriorityQueue;

pub struct LRUReplacer {
    // Saves the page_id with the timestamp as priority
    current_pages: PriorityQueue<usize, Reverse<i64>>,
}

impl LRUReplacer {
    /// Allocates a new LRUReplacer with a given capacity.
    pub fn new(capacity: usize) -> LRUReplacer {
        return LRUReplacer {
            current_pages: PriorityQueue::with_capacity(capacity),
        };
    }

    /// Returns the current number of available pages.
    pub fn current_size(&self) -> usize {
        return self.current_pages.len();
    }

    /// Adds a page_index to the available page indices and annotates it with the current
    /// timestamp.
    pub fn add_page(&mut self, page_index: usize) {
        let time_stamp = chrono::Utc::now().timestamp_millis();

        self.current_pages.push(page_index, Reverse(time_stamp));
    }

    /// Removes the page index from the available list and returns its index.
    /// Returns None if the page index was not present in the list.
    pub fn drop_page(&mut self, page_index: usize) -> Option<usize> {
        return self
            .current_pages
            .remove(&page_index)
            .and_then(|page| Some(page.0));
    }
    pub fn drop_all_pages(&mut self){
        self.current_pages.clear();
    }

    /// Removes and returns the least recently added page index, as in the page that has not been used for the
    /// longest.
    /// If there is no page available, [None] is returned.
    pub fn pop_least_recently_used(&mut self) -> Option<usize> {
        return self.current_pages.pop().and_then(|page| Some(page.0));
    }
}

#[cfg(test)]
mod lru_tests {
    use std::{thread::sleep, time::Duration};

    use super::LRUReplacer;

    #[test]
    fn pin_nonexisting() {
        let mut lru_replacer = LRUReplacer::new(10);

        assert_eq!(lru_replacer.drop_page(0), None);
    }
    #[test]
    fn pin_existing() {
        let mut lru_replacer = LRUReplacer::new(10);
        lru_replacer.add_page(0);

        assert_eq!(lru_replacer.drop_page(0), Some(0));
    }
    #[test]
    fn usual_get_victim() {
        let mut lru_replacer = LRUReplacer::new(10);
        let one_ms = Duration::from_millis(1);

        lru_replacer.add_page(0);
        sleep(one_ms);
        lru_replacer.add_page(2);
        sleep(one_ms);
        lru_replacer.add_page(1);

        assert_eq!(lru_replacer.pop_least_recently_used(), Some(0));
        assert_eq!(lru_replacer.pop_least_recently_used(), Some(2));
        assert_eq!(lru_replacer.pop_least_recently_used(), Some(1));
    }
}
