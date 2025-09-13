use std::ops::{Index, IndexMut};

use crate::ByteStore;
use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;

/// Configuration for resize behavior
#[derive(Debug, Clone)]
pub struct ResizeConfig {
    /// Load factor threshold to trigger resize (default: 0.75)
    pub load_factor_threshold: f64,

    /// Number of entries to rehash per operation (default: 8)
    pub rehash_batch_size: usize,

    /// Growth factor for new capacity (default: 2.0)
    pub growth_factor: f64,
}

impl Default for ResizeConfig {
    fn default() -> Self {
        Self {
            load_factor_threshold: 0.75,
            rehash_batch_size: 8,
            growth_factor: 2.0,
        }
    }
}

/// Double array entries for incremental resizing
///
/// This implementation maintains two entry arrays during resize operations:
/// - old_entries: Contains entries that haven't been rehashed yet
/// - new_entries: Contains rehashed entries and new insertions
///
/// Incremental rehashing distributes the resize work across multiple operations
/// to avoid large pauses.
#[derive(Debug)]
pub struct DoubleArrayEntries<BS: ByteStore> {
    /// The old entries array (present during resize)
    old_entries: Option<FixedVec<Entry, BS>>,

    /// The new entries array (always present)
    new_entries: FixedVec<Entry, BS>,

    /// Index of the next entry to rehash in old_entries
    /// Once all entries are rehashed, old_entries is dropped
    rehash_progress: usize,

    /// Number of occupied entries across both arrays
    occupied_count: usize,

    /// Configuration for resize behavior
    config: ResizeConfig,
}

impl<BS: ByteStore> DoubleArrayEntries<BS> {
    /// Creates a new DoubleArrayEntries in normal state (single array)
    pub fn new(entries: FixedVec<Entry, BS>) -> Self {
        Self::new_with_config(entries, ResizeConfig::default())
    }

    /// Creates a new DoubleArrayEntries with custom configuration
    pub fn new_with_config(entries: FixedVec<Entry, BS>, config: ResizeConfig) -> Self {
        let occupied_count = entries.iter().filter(|e| e.is_occupied()).count();
        Self {
            old_entries: None,
            new_entries: entries,
            rehash_progress: 0,
            occupied_count,
            config,
        }
    }

    /// Creates a DoubleArrayEntries directly with given capacity
    pub fn with_capacity(store: BS, capacity: usize) -> Result<Self> {
        // For VecStore, we need to ensure the store has the right size
        let entries = FixedVec::new(store);
        Ok(Self::new(entries))
    }

    /// Creates a DoubleArrayEntries in mid-resize state (for restoration from disk)
    pub fn load_from_state(
        old_entries: Option<FixedVec<Entry, BS>>,
        new_entries: FixedVec<Entry, BS>,
        rehash_progress: usize,
        config: ResizeConfig,
    ) -> Self {
        // Count occupied entries across both arrays
        let new_count = new_entries.iter().filter(|e| e.is_occupied()).count();
        let old_count = if let Some(ref old) = old_entries {
            old.iter()
                .filter(|e| e.is_occupied() && !e.is_moved())
                .count()
        } else {
            0
        };
        let occupied_count = new_count + old_count;

        let rehash_progress = if let Some(ref old) = old_entries {
            rehash_progress.min(old.capacity())
        } else {
            0
        };

        Self {
            old_entries,
            new_entries,
            rehash_progress,
            occupied_count,
            config,
        }
    }

    /// Creates an empty DoubleArrayEntries with given capacity
    pub fn empty_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new(store);
        Ok(Self::new(entries))
    }

    /// Returns true if currently in resize mode (has both old and new arrays)
    pub fn is_resizing(&self) -> bool {
        self.old_entries.is_some()
    }

    /// Returns the current rehash progress (number of old entries processed)
    pub fn get_rehash_progress(&self) -> usize {
        self.rehash_progress
    }

    /// Set the rehash progress (used when restoring from disk)
    pub fn set_rehash_progress(&mut self, progress: usize) {
        if let Some(ref old_entries) = self.old_entries {
            self.rehash_progress = progress.min(old_entries.capacity());
        } else {
            self.rehash_progress = 0;
        }
    }

    /// Returns the capacity of the current active array
    pub fn capacity(&self) -> usize {
        self.new_entries.capacity()
    }

    /// Returns the combined capacity during resize, or just new array capacity otherwise
    pub fn effective_capacity(&self) -> usize {
        if let Some(ref old_entries) = self.old_entries {
            self.new_entries.capacity() + old_entries.capacity()
        } else {
            self.new_entries.capacity()
        }
    }

    /// Returns the number of occupied entries
    pub fn occupied_count(&self) -> usize {
        self.occupied_count
    }

    /// Returns the current load factor
    pub fn load_factor(&self) -> f64 {
        if self.new_entries.capacity() == 0 {
            0.0
        } else {
            self.occupied_count as f64 / self.new_entries.capacity() as f64
        }
    }

    /// Returns true if a resize should be triggered based on load factor
    pub fn should_resize(&self) -> bool {
        !self.is_resizing() && self.load_factor() > self.config.load_factor_threshold
    }

    /// Returns the new array capacity for debugging
    pub fn new_array_capacity(&self) -> usize {
        self.new_entries.capacity()
    }

    /// Returns the old array capacity for debugging (if resizing)
    pub fn old_array_capacity(&self) -> Option<usize> {
        self.old_entries.as_ref().map(|old| old.capacity())
    }

    /// Returns a reference to the configuration
    pub fn config(&self) -> &ResizeConfig {
        &self.config
    }

    /// Updates the configuration
    pub fn set_config(&mut self, config: ResizeConfig) {
        self.config = config;
    }

    /// Get a reference to the new entries array (for testing)
    pub fn new_entries(&self) -> &FixedVec<Entry, BS> {
        &self.new_entries
    }

    /// Get a reference to the old entries array (for testing)
    pub fn old_entries(&self) -> Option<&FixedVec<Entry, BS>> {
        self.old_entries.as_ref()
    }

    /// Starts a resize operation by creating a new array and preserving the old one
    pub fn start_resize(&mut self, new_capacity: usize) -> Result<()> {
        if self.old_entries.is_some() {
            return Ok(()); // Already resizing
        }

        // Create new empty array with increased capacity
        let new_entries = if new_capacity > self.new_entries.capacity() {
            self.new_entries.new_empty(new_capacity)
        } else {
            // If requested capacity is not larger, still create a new array but with a larger size
            let actual_new_capacity = (self.new_entries.capacity() + 1).max(new_capacity * 2);
            self.new_entries.new_empty(actual_new_capacity)
        };

        // Move current entries to old_entries and replace with new array
        let old_entries = std::mem::replace(&mut self.new_entries, new_entries);
        self.old_entries = Some(old_entries);
        self.rehash_progress = 0;

        Ok(())
    }

    /// Performs incremental rehashing with a custom hash function
    pub fn incremental_rehash_with_hasher<F>(&mut self, hash_fn: F) -> usize
    where
        F: Fn(crate::HeapIdx, crate::HeapIdx) -> u64,
    {
        if self.old_entries.is_none() {
            return 0; // Not resizing
        }

        let entries_to_rehash = self.config.rehash_batch_size;
        let mut rehashed = 0;
        let old_capacity = self.old_entries.as_ref().unwrap().capacity();

        while rehashed < entries_to_rehash && self.rehash_progress < old_capacity {
            let entry = self.old_entries.as_ref().unwrap()[self.rehash_progress];

            if entry.is_occupied() && !entry.is_moved() {
                // Calculate proper hash using the provided hash function
                let hash = hash_fn(entry.key_pos(), entry.value_pos());

                // Find insertion slot in new array
                if let Ok(new_index) = self.find_insertion_slot(hash) {
                    self.new_entries[new_index] = entry;

                    // Mark the old entry as moved
                    self.old_entries.as_mut().unwrap()[self.rehash_progress].mark_as_moved();

                    rehashed += 1;
                }
            }

            self.rehash_progress += 1;
        }

        // If we've finished rehashing all entries, clean up the old array
        if self.rehash_progress >= old_capacity {
            self.old_entries = None;
            self.rehash_progress = 0;
        }

        rehashed
    }

    /// Performs incremental rehashing using key position as hash (for testing)
    pub fn incremental_rehash_simple(&mut self) -> usize {
        self.incremental_rehash_with_hasher(|key_pos, _| u64::from(key_pos))
    }

    /// Completes the resize operation by rehashing all remaining entries
    pub fn complete_resize<F>(&mut self, hash_fn: F) -> Result<()>
    where
        F: Fn(crate::HeapIdx, crate::HeapIdx) -> u64,
    {
        while self.is_resizing() {
            let rehashed = self.incremental_rehash_with_hasher(&hash_fn);
            if rehashed == 0 {
                break; // No more entries to rehash
            }
        }
        Ok(())
    }

    /// Finds an available insertion slot in the new entries array
    fn find_insertion_slot(&self, hash: u64) -> Result<usize> {
        let capacity = self.new_entries.capacity();
        if capacity == 0 {
            return Err(crate::error::DiskMapError::InvalidInput(
                "Cannot insert into zero-capacity array".to_string(),
            ));
        }

        let mut index = (hash as usize) % capacity;
        let start_index = index;

        // Linear probing to find empty slot
        loop {
            if !self.new_entries[index].is_occupied() {
                return Ok(index);
            }
            index = (index + 1) % capacity;
            if index == start_index {
                return Err(crate::error::DiskMapError::InvalidInput(
                    "No available slots in new array".to_string(),
                ));
            }
        }
    }

    /// Insert an entry into the appropriate array
    pub fn insert_entry(&mut self, hash: u64, entry: Entry) -> Result<usize> {
        // Always insert new entries into the new array
        let index = self.find_insertion_slot(hash)?;
        let old_occupied = self.new_entries[index].is_occupied();
        self.new_entries[index] = entry;

        if !old_occupied && entry.is_occupied() {
            self.occupied_count += 1;
        }

        Ok(index)
    }

    /// Find a slot for the given hash and key matcher
    pub fn find_slot<F>(&self, hash: u64, mut key_matcher: F) -> std::result::Result<usize, usize>
    where
        F: FnMut(&Entry) -> bool,
    {
        let new_capacity = self.new_entries.capacity();
        if new_capacity == 0 {
            return Err(0);
        }

        // First search the new array
        let mut index = hash as usize % new_capacity;
        let start_index = index;
        let mut empty_slot = None;

        loop {
            let entry = &self.new_entries[index];
            if entry.is_empty() {
                if empty_slot.is_none() {
                    empty_slot = Some(index);
                }
                break;
            }
            if !entry.is_deleted() && key_matcher(entry) {
                return Ok(index);
            }
            index = (index + 1) % new_capacity;
            if index == start_index {
                break;
            }
        }

        // If resizing, also check old array for any unrelocated entries
        if let Some(ref old_entries) = self.old_entries {
            let old_capacity = old_entries.capacity();
            let old_start_index = (hash as usize) % old_capacity;
            let mut old_index = old_start_index;

            for _ in 0..old_capacity {
                let entry = &old_entries[old_index];
                if entry.is_empty() {
                    break;
                }
                if !entry.is_deleted() && !entry.is_moved() && key_matcher(entry) {
                    // Return index offset by new_capacity to indicate it's in old array
                    return Ok(old_index + new_capacity);
                }
                old_index = (old_index + 1) % old_capacity;
                if old_index == old_start_index {
                    break;
                }
            }
        }

        // Key not found, return the first empty slot we found
        Err(empty_slot.unwrap_or(hash as usize % new_capacity))
    }

    /// Get an entry by index (handles both arrays during resize)
    pub fn get_entry(&self, index: usize) -> &Entry {
        &self[index]
    }

    /// Get a mutable entry by index (handles both arrays during resize)
    pub fn get_entry_mut(&mut self, index: usize) -> &mut Entry {
        &mut self[index]
    }

    /// Set an entry by index, updating occupied count appropriately
    pub fn set_entry(&mut self, index: usize, entry: Entry) {
        let old_entry = &self[index];
        let old_occupied = old_entry.is_occupied();
        let new_occupied = entry.is_occupied();

        self[index] = entry;

        // Update occupied count
        match (old_occupied, new_occupied) {
            (false, true) => self.occupied_count += 1,
            (true, false) => self.occupied_count -= 1,
            _ => {} // No change
        }
    }

    /// Iterator over all occupied entries
    pub fn occupied_entries(&self) -> impl Iterator<Item = (usize, &Entry)> {
        let new_entries_iter = (0..self.new_entries.capacity()).filter_map(move |i| {
            let entry = &self.new_entries[i];
            if entry.is_occupied() {
                Some((i, entry))
            } else {
                None
            }
        });

        let old_entries_iter = if let Some(ref old_entries) = self.old_entries {
            let new_capacity = self.new_entries.capacity();
            (0..old_entries.capacity())
                .filter_map(move |i| {
                    let entry = &old_entries[i];
                    if entry.is_occupied() && !entry.is_moved() {
                        Some((i + new_capacity, entry))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        new_entries_iter.chain(old_entries_iter.into_iter())
    }

    /// Creates a new empty instance with the same configuration
    pub fn new_empty(&self, capacity: usize) -> Self {
        let new_entries = if capacity > self.new_entries.capacity() {
            self.new_entries.new_empty(capacity)
        } else {
            // If requested capacity is not larger, create a new array with at least +1 capacity
            let actual_capacity = self.new_entries.capacity() + 1;
            self.new_entries.new_empty(actual_capacity)
        };
        Self {
            old_entries: None,
            new_entries,
            rehash_progress: 0,
            occupied_count: 0,
            config: self.config.clone(),
        }
    }
}

impl<BS: ByteStore> Index<usize> for DoubleArrayEntries<BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        let new_capacity = self.new_entries.capacity();

        if index < new_capacity {
            &self.new_entries[index]
        } else {
            let old_entries = self
                .old_entries
                .as_ref()
                .expect("Attempted to access old array when not resizing");
            let old_index = index - new_capacity;
            &old_entries[old_index]
        }
    }
}

impl<BS: ByteStore> IndexMut<usize> for DoubleArrayEntries<BS> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let new_capacity = self.new_entries.capacity();

        if index < new_capacity {
            &mut self.new_entries[index]
        } else {
            let old_entries = self
                .old_entries
                .as_mut()
                .expect("Attempted to access old array when not resizing");
            let old_index = index - new_capacity;
            &mut old_entries[old_index]
        }
    }
}
