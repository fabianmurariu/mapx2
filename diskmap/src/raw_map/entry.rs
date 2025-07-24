#![allow(dead_code)]
use bytemuck::{Pod, Zeroable};
use modular_bitfield::prelude::B62;
use modular_bitfield::{Specifier, bitfield};

#[derive(Specifier, PartialEq, Debug, Clone, Copy)]
pub enum Status {
    Empty,
    Full,
    Deleted,
    None,
}

#[bitfield(bits = 128)]
#[derive(Clone, Copy, Zeroable, Pod, Debug)]
#[repr(C)]
#[allow(dead_code)]
pub struct Entry {
    #[bits = 2]
    pub status: Status,
    pub k_pos: B62,
    pub v_pos: u64,
}

impl Entry {
    pub fn occupied_at_pos(k_pos: usize, v_pos: usize) -> Self {
        let k_pos_u64 = k_pos as u64;
        assert!(k_pos_u64 < (1 << 62));
        Entry::new()
            .with_status(Status::Full)
            .with_k_pos(k_pos_u64)
            .with_v_pos(v_pos as u64)
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

    pub fn set_new_kv(&mut self, k_pos: usize, v_pos: usize) {
        let k_pos_u64 = k_pos as u64;
        assert!(k_pos_u64 < (1 << 62));
        *self = Entry::new()
            .with_status(Status::Full)
            .with_k_pos(k_pos_u64)
            .with_v_pos(v_pos as u64);
    }
}
