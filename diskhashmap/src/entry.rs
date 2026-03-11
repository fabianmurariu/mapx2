//! Packed entry structure for inline storage.
//!
//! Entry layout (64 bits = 8 bytes):
//! - state: 2 bits (Empty, Full, Deleted, Moved)
//! - hash_prefix: 14 bits (for quick rejection before key comparison)
//! - pos: 48 bits (heap position)
//!
//! This compact representation enables:
//! - Better cache utilization (8 bytes vs 16 bytes)
//! - Quick hash prefix check before heap access (99.99% early rejection with 14 bits)
//! - 48-bit addressing (256 TB addressable space)

use bytemuck::{Pod, Zeroable};

/// Entry state encoded as 2 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    /// Slot has never been used
    Empty = 0b00,
    /// Slot contains a valid key-value pair
    Full = 0b01,
    /// Slot previously contained data but was deleted (tombstone)
    Deleted = 0b10,
    /// Entry has been migrated during incremental resizing
    Moved = 0b11,
}

impl Status {
    #[inline]
    fn from_bits(bits: u8) -> Self {
        match bits & 0b11 {
            0b00 => Status::Empty,
            0b01 => Status::Full,
            0b10 => Status::Deleted,
            0b11 => Status::Moved,
            _ => unreachable!(),
        }
    }
}

/// Packed entry: state(2 bits) | hash_prefix(14 bits) | pos(48 bits)
///
/// Bit layout (little-endian):
/// ```text
/// [0:1]   - state (2 bits)
/// [2:15]  - hash_prefix (14 bits, for quick rejection)
/// [16:63] - pos (48 bits)
/// ```
#[derive(Clone, Copy, Zeroable, Pod, PartialEq, Eq)]
#[repr(transparent)]
pub struct Entry(u64);

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("status", &self.status())
            .field("hash_prefix", &format!("{:04x}", self.hash_prefix()))
            .field("pos", &self.pos())
            .finish()
    }
}

impl Entry {
    /// Mask for the 2-bit state field (bits 0-1)
    const STATE_MASK: u64 = 0b11;
    /// Mask for the 14-bit hash prefix field (bits 2-15)
    const HASH_PREFIX_MASK: u64 = 0x3FFF << 2;
    /// Mask for the 48-bit pos field (bits 16-63)
    const POS_MASK: u64 = !0xFFFF;
    /// Maximum pos value (48 bits = 256 TB)
    pub const MAX_POS: u64 = (1u64 << 48) - 1;

    /// Create a new empty entry.
    #[inline]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Extract 14-bit hash prefix from a 64-bit hash.
    #[inline]
    pub const fn hash_to_prefix(hash: u64) -> u16 {
        // Use upper bits of hash (often have better distribution)
        (hash >> 50) as u16 & 0x3FFF
    }

    /// Create an occupied entry at the given heap position with hash prefix.
    #[inline]
    pub fn occupied_at(pos: u64, hash: u64) -> Self {
        debug_assert!(pos <= Self::MAX_POS, "pos exceeds 48 bits");
        let hash_prefix = Self::hash_to_prefix(hash) as u64;
        Self((Status::Full as u64) | (hash_prefix << 2) | (pos << 16))
    }

    /// Get the status.
    #[inline]
    pub fn status(&self) -> Status {
        Status::from_bits((self.0 & Self::STATE_MASK) as u8)
    }

    /// Set the status.
    #[inline]
    pub fn set_status(&mut self, status: Status) {
        self.0 = (self.0 & !Self::STATE_MASK) | (status as u64);
    }

    /// Get the status (alias for compatibility).
    #[inline]
    pub fn state(&self) -> Status {
        self.status()
    }

    /// Get the 14-bit hash prefix (for quick rejection).
    #[inline]
    pub fn hash_prefix(&self) -> u16 {
        ((self.0 & Self::HASH_PREFIX_MASK) >> 2) as u16
    }

    /// Check if hash prefix matches (for quick rejection before key comparison).
    #[inline]
    pub fn hash_prefix_matches(&self, hash: u64) -> bool {
        self.hash_prefix() == Self::hash_to_prefix(hash)
    }

    /// Get the heap position.
    #[inline]
    pub fn pos(&self) -> u64 {
        self.0 >> 16
    }

    /// Set the heap position.
    #[inline]
    pub fn set_pos(&mut self, pos: u64) {
        debug_assert!(pos <= Self::MAX_POS, "pos exceeds 48 bits");
        self.0 = (self.0 & !Self::POS_MASK) | (pos << 16);
    }

    /// Create a new entry with the given position (keeps status, hash_prefix).
    #[inline]
    pub fn with_pos(self, pos: u64) -> Self {
        debug_assert!(pos <= Self::MAX_POS, "pos exceeds 48 bits");
        Self((self.0 & !Self::POS_MASK) | (pos << 16))
    }

    /// Create a new entry with the given status (keeps hash_prefix and pos).
    #[inline]
    pub fn with_status(self, status: Status) -> Self {
        Self((self.0 & !Self::STATE_MASK) | (status as u64))
    }

    /// Check if the entry is occupied.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.status() == Status::Full
    }

    /// Check if the entry is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.status() == Status::Empty
    }

    /// Check if the entry is deleted (tombstone).
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.status() == Status::Deleted
    }

    /// Check if the entry has been moved during resize.
    #[inline]
    pub fn is_moved(&self) -> bool {
        self.status() == Status::Moved
    }

    /// Mark this entry as moved.
    #[inline]
    pub fn mark_as_moved(&mut self) {
        self.set_status(Status::Moved);
    }

    /// Mark this entry as deleted.
    #[inline]
    pub fn mark_as_deleted(&mut self) {
        self.set_status(Status::Deleted);
    }

    /// Get the raw u64 value (for persistence).
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Create from raw u64 value (for loading from persistence).
    #[inline]
    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_size() {
        assert_eq!(std::mem::size_of::<Entry>(), 8);
    }

    #[test]
    fn test_new_entry_is_empty() {
        let entry = Entry::new();
        assert!(entry.is_empty());
        assert!(!entry.is_occupied());
        assert!(!entry.is_deleted());
        assert!(!entry.is_moved());
        assert_eq!(entry.pos(), 0);
    }

    const TEST_HASH: u64 = 0xDEADBEEFCAFEBABE;

    #[test]
    fn test_occupied_entry() {
        let entry = Entry::occupied_at(12345, TEST_HASH);
        assert!(entry.is_occupied());
        assert!(!entry.is_empty());
        assert_eq!(entry.pos(), 12345);
        assert_eq!(entry.hash_prefix(), Entry::hash_to_prefix(TEST_HASH));
    }

    #[test]
    fn test_status_transitions() {
        let mut entry = Entry::occupied_at(100, TEST_HASH);
        assert!(entry.is_occupied());

        entry.mark_as_deleted();
        assert!(entry.is_deleted());
        assert_eq!(entry.pos(), 100); // pos preserved

        entry.set_status(Status::Moved);
        assert!(entry.is_moved());
        assert_eq!(entry.pos(), 100); // pos still preserved
    }

    #[test]
    fn test_pos_operations() {
        let mut entry = Entry::occupied_at(1000, TEST_HASH);

        entry.set_pos(2000);
        assert_eq!(entry.pos(), 2000);
        assert!(entry.is_occupied()); // status preserved
    }

    #[test]
    fn test_max_pos() {
        let max_pos = Entry::MAX_POS;
        let entry = Entry::occupied_at(max_pos, TEST_HASH);
        assert_eq!(entry.pos(), max_pos);
    }

    #[test]
    fn test_with_methods() {
        let entry = Entry::occupied_at(1000, TEST_HASH);

        let entry2 = entry.with_pos(2000);
        assert_eq!(entry2.pos(), 2000);
        assert!(entry2.is_occupied());

        let entry3 = entry.with_status(Status::Deleted);
        assert!(entry3.is_deleted());
        assert_eq!(entry3.pos(), 1000);
    }

    #[test]
    fn test_u64_roundtrip() {
        let entry = Entry::occupied_at(123456789, TEST_HASH);
        let raw = entry.as_u64();
        let restored = Entry::from_u64(raw);

        assert_eq!(restored.status(), entry.status());
        assert_eq!(restored.pos(), entry.pos());
        assert_eq!(restored.hash_prefix(), entry.hash_prefix());
    }

    #[test]
    fn test_bytemuck() {
        let entry = Entry::occupied_at(999, TEST_HASH);
        let bytes: &[u8] = bytemuck::bytes_of(&entry);
        assert_eq!(bytes.len(), 8);

        let restored: &Entry = bytemuck::from_bytes(bytes);
        assert_eq!(restored.status(), entry.status());
        assert_eq!(restored.pos(), entry.pos());
        assert_eq!(restored.hash_prefix(), entry.hash_prefix());
    }

    #[test]
    fn test_hash_prefix() {
        let hash1: u64 = 0xABCDEF0123456789;
        let hash2: u64 = 0xABCD000000000000; // Same upper 14 bits
        let hash3: u64 = 0x1234567890ABCDEF; // Different upper 14 bits

        let entry = Entry::occupied_at(100, hash1);
        assert!(entry.hash_prefix_matches(hash1));
        assert!(entry.hash_prefix_matches(hash2)); // Same prefix (upper 14 bits)
        assert!(!entry.hash_prefix_matches(hash3)); // Different prefix
    }

    #[test]
    fn test_hash_prefix_14_bits() {
        // Test that we're using 14 bits (values 0 to 16383)
        let max_prefix = Entry::hash_to_prefix(u64::MAX);
        assert_eq!(max_prefix, 0x3FFF); // 14 bits all set

        let entry = Entry::occupied_at(0, u64::MAX);
        assert_eq!(entry.hash_prefix(), 0x3FFF);
    }
}
