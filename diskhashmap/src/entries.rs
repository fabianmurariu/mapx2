use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;
use crate::ByteStore;

/// Configuration for resize behavior
#[derive(Debug, Clone)]
pub struct ResizeConfig {
    /// Load factor threshold to trigger resize (default: 0.75)
    pub load_factor_threshold: f64,
    
    /// Number of entries to rehash per operation (default: 8)
    pub rehash_batch_size: usize,
    
    /// Growth factor for new capacity (default: 2.0)
    pub growth_factor: f64,
    
    /// Whether to use double array implementation for incremental resizing
    pub use_double_array: bool,
}

impl Default for ResizeConfig {
    fn default() -> Self {
        Self {
            load_factor_threshold: 0.75,
            rehash_batch_size: 8,
            growth_factor: 2.0,
            use_double_array: false, // Default to single array for backward compatibility
        }
    }
}

/// Trait abstracting entry storage operations for DiskHashMap
/// 
/// This trait abstracts the operations performed on the entries array,
/// allowing for different implementations such as:
/// - Single array implementation (current `FixedVec<Entry, BS>`)
/// - Double array implementation for incremental resizing (future)
pub trait EntriesStorage<BS: ByteStore>: Index<usize, Output = Entry> + IndexMut<usize> {
    /// Returns the current capacity of the entries storage
    fn capacity(&self) -> usize;

    /// Returns the number of entries that are currently occupied
    fn occupied_count(&self) -> usize;

    /// Creates a new entries storage with the specified capacity
    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self>
    where
        Self: Sized;

    /// Creates a new empty entries storage with increased capacity
    fn new_empty(&self, new_capacity: usize) -> Self;

    /// Returns true if the storage is currently being resized (for double array impl)
    fn is_resizing(&self) -> bool {
        false
    }

    /// Returns the effective capacity during resize operations
    /// For single array: returns capacity()
    /// For double array: returns old_capacity + new_capacity during resize
    fn effective_capacity(&self) -> usize {
        self.capacity()
    }

    /// Starts a resize operation (for double array implementation)
    fn start_resize(&mut self, _new_capacity: usize) -> Result<()> {
        Ok(()) // No-op for single array
    }

    /// Performs incremental rehashing during resize (for double array impl)
    /// Returns the number of entries that were rehashed
    fn incremental_rehash(&mut self, _max_entries: usize) -> usize {
        0 // No-op for single array
    }

    /// Completes the resize operation (for double array implementation)
    fn complete_resize(&mut self) -> Result<()> {
        Ok(()) // No-op for single array
    }

    /// Gets an entry at the given index, handling resize state
    /// For single array: simple index access
    /// For double array: checks both old and new arrays based on resize state
    fn get_entry(&self, index: usize) -> &Entry {
        &self[index]
    }

    /// Gets a mutable entry at the given index, handling resize state
    fn get_entry_mut(&mut self, index: usize) -> &mut Entry {
        &mut self[index]
    }

    /// Sets an entry at the given index, handling resize state
    fn set_entry(&mut self, index: usize, entry: Entry) {
        self[index] = entry;
    }

    /// Iterator over all occupied entries (for iteration support)
    fn occupied_entries(&self) -> OccupiedEntriesIter<'_, Self, BS>
    where
        Self: Sized,
    {
        OccupiedEntriesIter {
            entries: self,
            current_index: 0,
            remaining: self.occupied_count(),
            _phantom: PhantomData,
        }
    }

    /// Finds the next probe index for the given hash and current index
    /// This method can be overridden for different probing strategies
    fn next_probe_index(&self, current: usize, _hash: u64) -> usize {
        (current + 1) % self.effective_capacity()
    }

    /// Calculates the starting probe index for the given hash
    fn probe_start(&self, hash: u64) -> usize {
        (hash as usize) % self.effective_capacity()
    }
}

/// Iterator over occupied entries in the entries storage
pub struct OccupiedEntriesIter<'a, E: EntriesStorage<BS> + ?Sized, BS: ByteStore> {
    entries: &'a E,
    current_index: usize,
    remaining: usize,
    _phantom: PhantomData<BS>,
}

impl<'a, E: EntriesStorage<BS>, BS: ByteStore> Iterator for OccupiedEntriesIter<'a, E, BS> {
    type Item = (usize, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        while self.current_index < self.entries.effective_capacity() {
            let entry = self.entries.get_entry(self.current_index);
            let index = self.current_index;
            self.current_index += 1;

            if entry.is_occupied() {
                self.remaining -= 1;
                return Some((index, entry));
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<E: EntriesStorage<BS>, BS: ByteStore> ExactSizeIterator for OccupiedEntriesIter<'_, E, BS> {
    fn len(&self) -> usize {
        self.remaining
    }
}

/// Single array implementation of EntriesStorage (current implementation)
pub struct SingleArrayEntries<BS: ByteStore> {
    entries: FixedVec<Entry, BS>,
    occupied_count: usize,
}

impl<BS: ByteStore> SingleArrayEntries<BS> {
    pub fn new(entries: FixedVec<Entry, BS>) -> Self {
        let occupied_count = entries.iter().filter(|e| e.is_occupied()).count();
        Self {
            entries,
            occupied_count,
        }
    }

    pub fn into_inner(self) -> FixedVec<Entry, BS> {
        self.entries
    }

    pub fn inner(&self) -> &FixedVec<Entry, BS> {
        &self.entries
    }

    pub fn inner_mut(&mut self) -> &mut FixedVec<Entry, BS> {
        &mut self.entries
    }
}

impl<BS: ByteStore> Index<usize> for SingleArrayEntries<BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl<BS: ByteStore> IndexMut<usize> for SingleArrayEntries<BS> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}

impl<BS: ByteStore> EntriesStorage<BS> for SingleArrayEntries<BS> {
    fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    fn occupied_count(&self) -> usize {
        self.occupied_count
    }

    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new(store);
        // Verify that the capacity matches what we expect
        assert!(
            entries.capacity() >= capacity,
            "Store capacity {} is less than requested capacity {}",
            entries.capacity(),
            capacity
        );
        Ok(Self {
            entries,
            occupied_count: 0,
        })
    }

    fn new_empty(&self, new_capacity: usize) -> Self {
        let new_entries = self.entries.new_empty(new_capacity);
        Self {
            entries: new_entries,
            occupied_count: 0,
        }
    }

    fn set_entry(&mut self, index: usize, entry: Entry) {
        let old_entry = &self.entries[index];
        let old_occupied = old_entry.is_occupied();
        let new_occupied = entry.is_occupied();

        self.entries[index] = entry;

        // Update occupied count
        match (old_occupied, new_occupied) {
            (false, true) => self.occupied_count += 1,
            (true, false) => self.occupied_count -= 1,
            _ => {} // No change
        }
    }
}

/// Direct implementation of EntriesStorage for FixedVec<Entry, BS>
impl<BS: ByteStore> Index<usize> for FixedVec<Entry, BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        &(**self)[index]
    }
}

impl<BS: ByteStore> IndexMut<usize> for FixedVec<Entry, BS> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut (**self)[index]
    }
}

impl<BS: ByteStore> EntriesStorage<BS> for FixedVec<Entry, BS> {
    fn capacity(&self) -> usize {
        FixedVec::capacity(self)
    }

    fn occupied_count(&self) -> usize {
        self.iter().filter(|e| e.is_occupied()).count()
    }

    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new(store);
        // Verify that the capacity matches what we expect
        if entries.capacity() < capacity {
            return Err(crate::error::DiskMapError::InvalidInput(format!(
                "Store capacity {} is less than requested capacity {}",
                entries.capacity(),
                capacity
            )));
        }
        Ok(entries)
    }

    fn new_empty(&self, new_capacity: usize) -> Self {
        self.new_empty(new_capacity)
    }

    fn set_entry(&mut self, index: usize, entry: Entry) {
        self[index] = entry;
    }
}

/// Enum to hold different entry storage implementations
/// This allows switching between single and double array implementations
#[derive(Debug)]
pub enum EntriesImpl<BS: ByteStore> {
    Single(FixedVec<Entry, BS>),
    Double(DoubleArrayEntries<BS>),
}

impl<BS: ByteStore> EntriesImpl<BS> {
    /// Creates a new EntriesImpl with the specified configuration
    pub fn new_with_config(store: BS, capacity: usize, config: &ResizeConfig) -> Result<Self> {
        if config.use_double_array {
            let entries = FixedVec::new_with_capacity(store, capacity)?;
            Ok(EntriesImpl::Double(DoubleArrayEntries::new(entries, config.rehash_batch_size)))
        } else {
            Ok(EntriesImpl::Single(FixedVec::new_with_capacity(store, capacity)?))
        }
    }
}

impl<BS: ByteStore> Index<usize> for EntriesImpl<BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            EntriesImpl::Single(entries) => &entries[index],
            EntriesImpl::Double(entries) => &entries[index],
        }
    }
}

impl<BS: ByteStore> IndexMut<usize> for EntriesImpl<BS> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            EntriesImpl::Single(entries) => &mut entries[index],
            EntriesImpl::Double(entries) => &mut entries[index],
        }
    }
}

impl<BS: ByteStore> EntriesStorage<BS> for EntriesImpl<BS> {
    fn capacity(&self) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.capacity(),
            EntriesImpl::Double(entries) => entries.capacity(),
        }
    }

    fn occupied_count(&self) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.occupied_count(),
            EntriesImpl::Double(entries) => entries.occupied_count(),
        }
    }

    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        // Default to single array implementation
        Ok(EntriesImpl::Single(FixedVec::new_with_capacity(store, capacity)?))
    }

    fn new_empty(&self, new_capacity: usize) -> Self {
        match self {
            EntriesImpl::Single(entries) => EntriesImpl::Single(entries.new_empty(new_capacity)),
            EntriesImpl::Double(entries) => EntriesImpl::Double(entries.new_empty(new_capacity)),
        }
    }

    fn is_resizing(&self) -> bool {
        match self {
            EntriesImpl::Single(entries) => entries.is_resizing(),
            EntriesImpl::Double(entries) => entries.is_resizing(),
        }
    }

    fn effective_capacity(&self) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.effective_capacity(),
            EntriesImpl::Double(entries) => entries.effective_capacity(),
        }
    }

    fn start_resize(&mut self, new_capacity: usize) -> Result<()> {
        match self {
            EntriesImpl::Single(entries) => entries.start_resize(new_capacity),
            EntriesImpl::Double(entries) => entries.start_resize(new_capacity),
        }
    }

    fn incremental_rehash(&mut self, max_entries: usize) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.incremental_rehash(max_entries),
            EntriesImpl::Double(entries) => entries.incremental_rehash(max_entries),
        }
    }

    fn complete_resize(&mut self) -> Result<()> {
        match self {
            EntriesImpl::Single(entries) => entries.complete_resize(),
            EntriesImpl::Double(entries) => entries.complete_resize(),
        }
    }

    fn get_entry(&self, index: usize) -> &Entry {
        match self {
            EntriesImpl::Single(entries) => entries.get_entry(index),
            EntriesImpl::Double(entries) => entries.get_entry(index),
        }
    }

    fn get_entry_mut(&mut self, index: usize) -> &mut Entry {
        match self {
            EntriesImpl::Single(entries) => entries.get_entry_mut(index),
            EntriesImpl::Double(entries) => entries.get_entry_mut(index),
        }
    }

    fn set_entry(&mut self, index: usize, entry: Entry) {
        match self {
            EntriesImpl::Single(entries) => entries.set_entry(index, entry),
            EntriesImpl::Double(entries) => entries.set_entry(index, entry),
        }
    }

    fn next_probe_index(&self, current: usize, hash: u64) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.next_probe_index(current, hash),
            EntriesImpl::Double(entries) => entries.next_probe_index(current, hash),
        }
    }

    fn probe_start(&self, hash: u64) -> usize {
        match self {
            EntriesImpl::Single(entries) => entries.probe_start(hash),
            EntriesImpl::Double(entries) => entries.probe_start(hash),
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
/// to avoid large pauses, following the algorithm outlined in docs/entries.md
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
    
    /// Number of entries to rehash per operation
    rehash_batch_size: usize,
}

impl<BS: ByteStore> DoubleArrayEntries<BS> {
    /// Creates a new DoubleArrayEntries in normal state (single array)
    pub fn new(entries: FixedVec<Entry, BS>, rehash_batch_size: usize) -> Self {
        let occupied_count = entries.iter().filter(|e| e.is_occupied()).count();
        Self {
            old_entries: None,
            new_entries: entries,
            rehash_progress: 0,
            occupied_count,
            rehash_batch_size,
        }
    }

    /// Starts a resize operation by creating a new array and preserving the old one
    fn start_resize_internal(&mut self, new_capacity: usize) -> Result<()> {
        if self.old_entries.is_some() {
            return Ok(()) // Already resizing
        }

        // Create new empty array with increased capacity
        let new_entries = self.new_entries.new_empty(new_capacity);
        
        // Move current entries to old_entries and replace with new array
        let old_entries = std::mem::replace(&mut self.new_entries, new_entries);
        self.old_entries = Some(old_entries);
        self.rehash_progress = 0;
        
        Ok(())
    }

    /// Performs incremental rehashing of entries from old array to new array
    /// Note: This needs access to the heap to recalculate hashes from key bytes
    /// For now, we use key position as a simple hash approximation
    fn incremental_rehash_internal(&mut self, max_entries: usize) -> Result<usize> {
        let old_entries = match &self.old_entries {
            Some(old) => old,
            None => return Ok(0), // Not resizing
        };

        let mut rehashed = 0;
        let old_capacity = old_entries.capacity();

        while rehashed < max_entries && self.rehash_progress < old_capacity {
            let entry = old_entries[self.rehash_progress];
            
            if entry.is_occupied() {
                // TODO: In a real implementation, we would:
                // 1. Get key bytes from heap using entry.key_pos()
                // 2. Calculate actual hash from key bytes
                // 3. Find insertion slot in new array using proper hash
                // For now, use key position as a simple hash approximation
                let hash = u64::from(entry.key_pos());
                
                // Find insertion slot in new array
                let new_index = self.find_insertion_slot(hash)?;
                self.new_entries[new_index] = entry;
                rehashed += 1;
            }
            
            self.rehash_progress += 1;
        }

        // Check if resize is complete
        if self.rehash_progress >= old_capacity {
            self.old_entries = None; // Drop old array
            self.rehash_progress = 0;
        }

        Ok(rehashed)
    }

    /// Finds an available insertion slot in the new entries array
    fn find_insertion_slot(&self, hash: u64) -> Result<usize> {
        let capacity = self.new_entries.capacity();
        let mut index = (hash as usize) % capacity;
        
        // Linear probing to find empty slot
        loop {
            if !self.new_entries[index].is_occupied() {
                return Ok(index);
            }
            index = (index + 1) % capacity;
        }
    }

    /// Looks up an entry in both arrays (new first, then old if resizing)
    #[allow(dead_code)]
    fn lookup_in_arrays(&self, hash: u64, key_matcher: impl Fn(&Entry) -> bool) -> Option<usize> {
        // First check new array
        let new_capacity = self.new_entries.capacity();
        let mut index = (hash as usize) % new_capacity;
        
        loop {
            let entry = &self.new_entries[index];
            if !entry.is_occupied() {
                break; // Empty slot, not found in new array
            }
            if key_matcher(entry) {
                return Some(index);
            }
            index = (index + 1) % new_capacity;
        }

        // If resizing, check old array for unrelocated entries
        if let Some(ref old_entries) = self.old_entries {
            let old_capacity = old_entries.capacity();
            let old_start_index = (hash as usize) % old_capacity;
            
            // Only search in old array if this hash bucket hasn't been rehashed yet
            if old_start_index >= self.rehash_progress {
                let mut old_index = old_start_index;
                loop {
                    let entry = &old_entries[old_index];
                    if !entry.is_occupied() {
                        break; // Empty slot, not found
                    }
                    if key_matcher(entry) {
                        // Return offset index to distinguish from new array
                        return Some(old_index + new_capacity);
                    }
                    old_index = (old_index + 1) % old_capacity;
                }
            }
        }

        None
    }
}

impl<BS: ByteStore> Index<usize> for DoubleArrayEntries<BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        let new_capacity = self.new_entries.capacity();
        
        if index < new_capacity {
            // Index is in new array
            &self.new_entries[index]
        } else {
            // Index is in old array (offset by new_capacity)
            let old_entries = self.old_entries.as_ref()
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
            // Index is in new array
            &mut self.new_entries[index]
        } else {
            // Index is in old array (offset by new_capacity)
            let old_entries = self.old_entries.as_mut()
                .expect("Attempted to access old array when not resizing");
            let old_index = index - new_capacity;
            &mut old_entries[old_index]
        }
    }
}

impl<BS: ByteStore> EntriesStorage<BS> for DoubleArrayEntries<BS> {
    fn capacity(&self) -> usize {
        self.new_entries.capacity()
    }

    fn occupied_count(&self) -> usize {
        self.occupied_count
    }

    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new_with_capacity(store, capacity)?;
        Ok(Self::new(entries, 8)) // Default batch size of 8
    }

    fn new_empty(&self, new_capacity: usize) -> Self {
        let new_entries = self.new_entries.new_empty(new_capacity);
        Self {
            old_entries: None,
            new_entries,
            rehash_progress: 0,
            occupied_count: 0,
            rehash_batch_size: self.rehash_batch_size,
        }
    }

    fn is_resizing(&self) -> bool {
        self.old_entries.is_some()
    }

    fn effective_capacity(&self) -> usize {
        if let Some(ref old_entries) = self.old_entries {
            // During resize: return combined capacity
            self.new_entries.capacity() + old_entries.capacity()
        } else {
            // Normal state: return new array capacity
            self.new_entries.capacity()
        }
    }

    fn start_resize(&mut self, new_capacity: usize) -> Result<()> {
        self.start_resize_internal(new_capacity)
    }

    fn incremental_rehash(&mut self, max_entries: usize) -> usize {
        // Use the configured batch size, but respect the max_entries limit
        let entries_to_rehash = std::cmp::min(max_entries, self.rehash_batch_size);
        self.incremental_rehash_internal(entries_to_rehash).unwrap_or(0)
    }

    fn complete_resize(&mut self) -> Result<()> {
        // Finish any remaining rehashing
        if let Some(ref old_entries) = self.old_entries {
            let remaining = old_entries.capacity() - self.rehash_progress;
            self.incremental_rehash_internal(remaining)?;
        }
        Ok(())
    }

    fn get_entry(&self, index: usize) -> &Entry {
        &self[index]
    }

    fn get_entry_mut(&mut self, index: usize) -> &mut Entry {
        &mut self[index]
    }

    fn set_entry(&mut self, index: usize, entry: Entry) {
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

    fn next_probe_index(&self, current: usize, _hash: u64) -> usize {
        let capacity = if current < self.new_entries.capacity() {
            // Probing in new array
            self.new_entries.capacity()
        } else {
            // Probing in old array
            self.old_entries.as_ref()
                .map(|old| old.capacity())
                .unwrap_or(self.new_entries.capacity())
        };
        
        let next = (current + 1) % capacity;
        
        // If we're in old array, maintain the offset
        if current >= self.new_entries.capacity() {
            next + self.new_entries.capacity()
        } else {
            next
        }
    }

    fn probe_start(&self, hash: u64) -> usize {
        // Always start probing in the new array
        (hash as usize) % self.new_entries.capacity()
    }
}