use std::sync::Arc;

use anyhow::anyhow;
use bytes::Buf;

use super::ColumnSeekPosition;
use crate::storage::checksum::verify_checksum;
use crate::storage::{BlockIndex, ChecksumType, StorageError, StorageResult};

pub const SECONDARY_INDEX_MAGIC: u32 = 0x2333;
pub const INDEX_FOOTER_SIZE: usize = 4 + 8 + 4 + 8;

#[derive(Clone)]
pub struct ColumnIndex {
    indexes: Arc<[BlockIndex]>,
}

impl ColumnIndex {
    pub fn new(indexes: Vec<BlockIndex>) -> Self {
        Self {
            indexes: indexes.into(),
        }
    }

    pub fn index(&self, block_id: u32) -> &BlockIndex {
        &self.indexes[block_id as usize]
    }

    pub fn indexes(&self) -> &[BlockIndex] {
        &*self.indexes
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    pub fn from_bytes(data: &[u8]) -> StorageResult<Self> {
        // TODO(chi): error handling
        let mut index_data = &data[..data.len() - INDEX_FOOTER_SIZE];
        let mut footer = &data[data.len() - INDEX_FOOTER_SIZE..];
        if footer.get_u32() != SECONDARY_INDEX_MAGIC {
            return Err(StorageError(anyhow!(
                "failed to decode column index: invalid magic"
            )));
        }
        let length = footer.get_u64() as usize;
        let checksum_type = ChecksumType::from_i32(footer.get_i32())?;
        let checksum = footer.get_u64();
        verify_checksum(checksum_type, index_data, checksum)?;

        let mut indexes = vec![];
        for _ in 0..length {
            let index = BlockIndex::decode(&mut index_data);
            indexes.push(index);
        }

        Ok(Self {
            indexes: indexes.into(),
        })
    }

    pub fn block_of_seek_position(&self, seek_pos: ColumnSeekPosition) -> u32 {
        match seek_pos {
            ColumnSeekPosition::RowId(row_id) => self.block_of_row(row_id),
            ColumnSeekPosition::SortKey(_) => todo!(),
        }
    }

    /// Find corresponding block of a row.
    pub fn block_of_row(&self, rowid: u32) -> u32 {
        // For example, there are 3 blocks, each of which has a first rowid of `233`, `2333`,
        // `23333`.
        //
        // ```plain
        // | 0 | 233 | 2333 | 23333 |
        // ```
        //
        // And now we want to find row x inside all these blocks. x is in a block `i` if:
        // first_row_id[i] <= x < first_row_id[i + 1]
        // Therefore, we partition the blocks by `first_row_id <= x`, and we can find the block
        // at `partition_point - 1`.

        let pp = self
            .indexes
            .partition_point(|index| index.first_rowid <= rowid) as u32;

        if pp == 0 {
            unreachable!()
        }

        pp - 1
    }
}
