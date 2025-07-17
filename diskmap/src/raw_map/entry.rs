use bytemuck::{Pod, Zeroable};
use modular_bitfield::prelude::B62;
use modular_bitfield::{Specifier, bitfield};

#[derive(Specifier, PartialEq)]
pub enum Status {
    Empty,
    Full,
    Deleted,
    None,
}

#[bitfield(bits = 128)]
#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct Entry {
    #[bits = 2]
    status: Status,
    k_pos: B62,
    v_pos: u64,
}

impl Entry {
    pub fn occupied_at_pos(k_pos: usize, v_pos: usize) -> Self {
        assert!(pos < (2 ^ 62) - 1);
        Entry::new()
            .with_status(Status::Full)
            .with_k_pos(k_pos as u64)
            .with_v_pos(v_pos)
    }

    pub fn is_occupied(&self) -> bool {
        self.status() == Status::Full
    }

    pub fn is_empty(&self) -> bool {
        self.status() == Status::Empty
    }

    pub fn is_deleted(&self) -> bool {
        self.status() == Status::Deleted
    }

    pub fn key_pos(&self) -> usize {
        self.k_pos() as usize
    }

    pub fn value_pos(&self) -> usize {
        self.v_pos() as usize
    }
}
