#![allow(dead_code)]

use super::rowset::MemRowset;
use super::{DiskTableRef, StorageResult};
use crate::array::DataChunk;

/// [`TableTransaction`] records the state of a single table. All operations (insert, update,
/// delete) should go through [`TableTransaction`].
pub struct TableTransaction {
    mem_rowset: Option<MemRowset>,
    read_only: bool,
    update: bool,
    table: DiskTableRef,
}

impl TableTransaction {
    /// Start a [`WriteBatch`]
    pub async fn start(table: DiskTableRef, read_only: bool, update: bool) -> StorageResult<Self> {
        Ok(Self {
            mem_rowset: None,
            table,
            update,
            read_only,
        })
    }

    /// Flush [`WriteBatch`] to some on-disk RowSets.
    pub async fn flush(self) {
        todo!()
    }

    /// Add a [`DataChunk`] to the mem rowset
    pub fn append(&self, _chunk: DataChunk) -> StorageResult<()> {
        todo!()
    }

    /// Delete a row from the table.
    async fn delete(&mut self, _row_id: u64) -> StorageResult<()> {
        todo!()
    }

    /// Commit all changes in this transaction.
    pub fn commit(self) -> StorageResult<()> {
        todo!()
    }

    /// Abort all changes in this transaction.
    pub fn abort(self) -> StorageResult<()> {
        todo!()
    }

    /// Create an iterator on this table.
    pub async fn scan(&self) {
        todo!()
    }
}
