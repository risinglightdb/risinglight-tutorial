//! On-disk representation of some enums

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    PrimitiveNonNull,
}

impl BlockType {
    pub fn from_i32(item: i32) -> Result<Self> {
        match item {
            1 => Ok(Self::PrimitiveNonNull),
            other => Err(anyhow!("invlid block type {}", other)),
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
}

impl ChecksumType {
    pub fn from_i32(item: i32) -> Result<Self> {
        match item {
            1 => Ok(Self::None),
            other => Err(anyhow!("invlid checksum type {}", other)),
        }
    }
}

impl From<ChecksumType> for i32 {
    fn from(ty: ChecksumType) -> Self {
        match ty {
            ChecksumType::None => 1,
        }
    }
}
