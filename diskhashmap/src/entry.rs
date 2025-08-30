#![allow(dead_code)]
use bytemuck::{Pod, Zeroable};
use modular_bitfield::prelude::B2;
use modular_bitfield::{Specifier, bitfield};

use crate::HeapIdx;

#[derive(Specifier, PartialEq, Debug, Clone, Copy)]
pub enum Status {
    Empty,
    Full,
    Deleted,
    Moved,
}

#[bitfield(bits = 4)]
#[derive(Clone, Copy, Zeroable, Pod, Debug, Specifier)]
#[repr(C)]
pub struct PaddedStatus {
    #[bits = 2]
    status: Status,
    #[bits = 2]
    padding: B2,
}

#[bitfield(bits = 128)]
#[derive(Clone, Copy, Zeroable, Pod, Debug, Specifier)]
#[repr(C)]
pub struct Entry {
    #[bits = 4]
    pub status: PaddedStatus,
    #[bits = 62]
    pub k_pos: HeapIdx,
    #[bits = 62]
    pub v_pos: HeapIdx,
}

impl Entry {
    pub fn occupied_at_pos(k_pos: HeapIdx, v_pos: HeapIdx) -> Self {
        Entry::new()
            .with_status(PaddedStatus::new().with_status(Status::Full))
            .with_k_pos(k_pos)
            .with_v_pos(v_pos)
    }

    pub fn is_occupied(&self) -> bool {
        self.status().status() == Status::Full
    }

    pub fn is_empty(&self) -> bool {
        self.status().status() == Status::Empty
    }

    pub fn is_deleted(&self) -> bool {
        self.status().status() == Status::Deleted
    }

    pub fn is_moved(&self) -> bool {
        self.status().status() == Status::Moved
    }

    pub fn key_pos(&self) -> HeapIdx {
        self.k_pos()
    }

    pub fn value_pos(&self) -> HeapIdx {
        self.v_pos()
    }

    pub fn set_new_kv(&mut self, k_pos: HeapIdx, v_pos: HeapIdx) {
        *self = Entry::new()
            .with_status(PaddedStatus::new().with_status(Status::Full))
            .with_k_pos(k_pos)
            .with_v_pos(v_pos);
    }

    pub fn mark_as_moved(&mut self) {
        *self = self.with_status(PaddedStatus::new().with_status(Status::Moved));
    }
}
