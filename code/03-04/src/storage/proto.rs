//! On-disk representation of some enums

use anyhow::anyhow;
use bytes::{Buf, BufMut};

use super::{StorageError, StorageResult};

#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    PrimitiveNonNull,
}

impl BlockType {
    pub fn from_i32(item: i32) -> StorageResult<Self> {
        match item {
            1 => Ok(Self::PrimitiveNonNull),
            other => Err(StorageError(anyhow!("invlid block type {}", other))),
        }
    }
}

impl From<BlockType> for i32 {
    fn from(ty: BlockType) -> Self {
        match ty {
            BlockType::PrimitiveNonNull => 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChecksumType {
    None,
    Crc32,
}

impl ChecksumType {
    pub fn from_i32(item: i32) -> StorageResult<Self> {
        match item {
            1 => Ok(Self::None),
            2 => Ok(Self::Crc32),
            other => Err(StorageError(anyhow!("invlid checksum type {}", other))),
        }
    }
}

impl From<ChecksumType> for i32 {
    fn from(ty: ChecksumType) -> Self {
        match ty {
            ChecksumType::None => 1,
            ChecksumType::Crc32 => 2,
        }
    }
}

/// Index of a block, which contains necessary information to identify what's inside a block.
#[derive(Clone)]
pub struct BlockIndex {
    pub offset: u64,
    pub length: u64,
    pub first_rowid: u32,
    pub row_count: u32,
}

impl BlockIndex {
    pub fn encode(&self, mut buf: impl BufMut) {
        buf.put_u64(self.offset);
        buf.put_u64(self.length);
        buf.put_u32(self.first_rowid);
        buf.put_u32(self.row_count);
    }

    pub fn decode(mut buf: impl Buf) -> Self {
        let offset = buf.get_u64();
        let length = buf.get_u64();
        let first_rowid = buf.get_u32();
        let row_count = buf.get_u32();
        Self {
            offset,
            length,
            first_rowid,
            row_count,
        }
    }
}
