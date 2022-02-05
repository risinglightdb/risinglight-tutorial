// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Secondary's [`Column`] builders and iterators.
//!
//! A column stores data of the same kind, e.g. Int32. On the storage format
//! side, a column is composed of multiple blocks and an index. The type of
//! blocks might not be the same. For example, a column could contains several
//! compressed blocks, and several RLE blocks.

mod column_builder;
mod column_index;
mod column_index_builder;
mod column_iterator;
mod concrete_column_iterator;
mod primitive_column_builder;

#[cfg(test)]
mod tests;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
pub use column_builder::*;
pub use column_iterator::*;
pub use concrete_column_iterator::*;
pub use primitive_column_builder::*;
pub use self::column_index::*;

use super::checksum::verify_checksum;
use super::{
    Block, BlockHeader, BlockIndex, ChecksumType, StorageError, StorageResult, BLOCK_HEADER_SIZE,
};
use crate::array::Array;

/// Stores information of a column
pub struct Column {
    index: ColumnIndex,
    /// We temporarily store column in-memory.
    data: Bytes,
}

/// Options for `ColumnBuilder`s.
#[derive(Clone)]
pub struct ColumnBuilderOptions {
    /// Target size (in bytes) of blocks
    pub target_block_size: usize,

    /// Checksum type used by columns
    pub checksum_type: ChecksumType,
}

impl ColumnBuilderOptions {
    #[cfg(test)]
    pub fn default_for_block_test() -> Self {
        Self {
            target_block_size: 128,
            checksum_type: ChecksumType::None,
        }
    }

    #[cfg(test)]
    pub fn default_for_column_test() -> Self {
        Self {
            target_block_size: 4096,
            checksum_type: ChecksumType::None,
        }
    }
}

/// Builds a column. [`ColumnBuilder`] will automatically chunk [`Array`] into
/// blocks, calls `BlockBuilder` to generate a block, and builds index for a
/// column. Note that one [`Array`] might require multiple [`ColumnBuilder`] to build.
///
/// * For nullable columns, there will be a bitmap file built with `BitmapColumnBuilder`.
/// * And for concrete data, there will be another column builder with concrete block builder.
///
/// After a single column has been built, an index file will also be generated with `IndexBuilder`.
pub trait ColumnBuilder<A: Array>: 'static + Send {
    /// Append an [`Array`] to the column. [`ColumnBuilder`] will automatically chunk it into
    /// small parts.
    fn append(&mut self, array: &A);

    /// Finish a column, return block index information and encoded block data
    fn finish(self) -> (Vec<BlockIndex>, Vec<u8>);
}

/// Iterator on a column. This iterator may request data from disk while iterating.
#[async_trait]
pub trait ColumnIterator<A: Array>: 'static + Send + Sync {
    /// Get a batch and the starting row id from the column. A `None` return value means that
    /// there are no more elements from the block. By using `expected_size`, developers can
    /// get an array of NO MORE THAN the `expected_size` on supported column types.
    async fn next_batch(&mut self, expected_size: Option<usize>)
        -> StorageResult<Option<(u32, A)>>;

    /// Number of items that can be fetched without I/O. When the column iterator has finished
    /// iterating, the returned value should be 0.
    fn fetch_hint(&self) -> usize;
}

/// When creating an iterator, a [`ColumnSeekPosition`] should be set as the initial location.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum ColumnSeekPosition {
    RowId(u32),
    SortKey(()),
}

impl ColumnSeekPosition {
    pub fn start() -> Self {
        Self::RowId(0)
    }
}

impl Column {
    pub fn new(index: ColumnIndex, data: Bytes) -> Self {
        Self { index, data }
    }

    pub fn index(&self) -> &ColumnIndex {
        &self.index
    }

    pub fn on_disk_size(&self) -> u64 {
        let lst_idx = self.index.index(self.index.len() as u32 - 1);
        lst_idx.offset + lst_idx.length
    }

    pub async fn get_block(&self, block_id: u32) -> StorageResult<(BlockHeader, Block)> {
        let info = self.index.index(block_id);
        let block = self
            .data
            .slice(info.offset as usize..(info.offset as usize + info.length as usize));

        if block.len() < BLOCK_HEADER_SIZE {
            return Err(StorageError(anyhow!("block is smaller than header size",)));
        }
        let mut header = &block[..BLOCK_HEADER_SIZE];
        let block_data = &block[BLOCK_HEADER_SIZE..];
        let block_header = BlockHeader::decode(&mut header).map_err(StorageError)?;

        verify_checksum(
            block_header.checksum_type,
            block_data,
            block_header.checksum,
        )?;

        Ok((block_header, block.slice(BLOCK_HEADER_SIZE..)))
    }
}