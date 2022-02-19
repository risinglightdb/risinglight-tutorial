//! On-disk storage

use std::sync::Arc;

use crate::array::DataChunk;
use crate::catalog::TableRefId;

/// The error type of storage operations.
#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
pub struct StorageError(#[from] anyhow::Error);

/// A specialized `Result` type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub type StorageRef = Arc<DiskStorage>;
pub type StorageTableRef = Arc<DiskTable>;

/// On-disk storage.
#[derive(Clone)]
pub struct DiskStorage;

/// An on-disk table.
pub struct DiskTable {
    #[allow(dead_code)]
    id: TableRefId,
}

impl Default for DiskStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        DiskStorage
    }

    /// Add a table.
    pub fn add_table(&self, _id: TableRefId) -> StorageResult<()> {
        todo!()
    }

    /// Get a table.
    pub fn get_table(&self, _id: TableRefId) -> StorageResult<StorageTableRef> {
        todo!()
    }
}

impl DiskTable {
    /// Append a chunk to the table.
    pub async fn append(&self, _chunk: DataChunk) -> StorageResult<()> {
        todo!()
    }

    /// Get all chunks of the table.
    pub async fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        todo!()
    }
}
