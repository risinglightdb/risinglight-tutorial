//! Persistent storage on disk.
//!
//! RisingLight's in-memory representation of data is very simple. Currently,
//! it is simple a vector of `DataChunk`. Upon insertion, users' data are
//! simply appended to the end of the vector.

// Temporarily enable allow dead code, so as to reduce warning. Should remove this when all
// tutorials complete.
#![allow(dead_code)]

mod block;
mod checksum;
mod column;
mod encode;
mod proto;
mod rowset;
mod table_transaction;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::anyhow;
pub use block::*;
pub use encode::*;
pub use proto::*;
pub use rowset::*;
pub use table_transaction::*;
pub use column::*;

use crate::array::DataChunk;
use crate::catalog::TableRefId;

/// The error type of storage operations.
#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
pub struct StorageError(anyhow::Error);

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self(err.into())
    }
}

/// A specialized `Result` type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub type StorageRef = Arc<DiskStorage>;
pub type DiskTableRef = Arc<DiskTable>;

/// Reference to a column.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum StorageColumnRef {
    /// A runtime column which contains necessary information to locate a row
    /// **only valid in the current transaction**.
    RowHandler,
    /// User column index. Note that this index is NOT the `ColumnId` in catalog. It is the storage
    /// column id, which is the same as the position of a column in the column catalog passed to a
    /// RowSet.
    Idx(u32),
}

/// Persistent storage on disk.
pub struct DiskStorage {
    tables: Mutex<HashMap<TableRefId, DiskTableRef>>,
}

impl Default for DiskStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskStorage {
    /// Create a new persistent storage on disk.
    pub fn new() -> Self {
        DiskStorage {
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Add a table.
    pub fn add_table(&self, id: TableRefId) -> StorageResult<()> {
        let table = Arc::new(DiskTable::new(id));
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    /// Get a table.
    pub fn get_table(&self, id: TableRefId) -> StorageResult<DiskTableRef> {
        self.tables
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("table not found: {:?}", id))
            .map_err(StorageError)
    }
}

/// A table in in-memory engine.
pub struct DiskTable {
    id: TableRefId,
    inner: RwLock<DiskTableInner>,
}

#[derive(Default)]
struct DiskTableInner {
    chunks: Vec<DataChunk>,
}

impl DiskTable {
    fn new(id: TableRefId) -> Self {
        Self {
            id,
            inner: RwLock::new(DiskTableInner::default()),
        }
    }

    async fn write(self: &Arc<Self>) -> StorageResult<TableTransaction> {
        Ok(TableTransaction::start(self.clone(), false, false).await?)
    }

    async fn read(self: &Arc<Self>) -> StorageResult<TableTransaction> {
        Ok(TableTransaction::start(self.clone(), true, false).await?)
    }

    async fn update(self: &Arc<Self>) -> StorageResult<TableTransaction> {
        Ok(TableTransaction::start(self.clone(), false, true).await?)
    }

    /// Append a chunk to the table.
    ///
    /// This interface will be deprecated soon in this tutorial.
    pub fn append(&self, chunk: DataChunk) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.chunks.push(chunk);
        Ok(())
    }

    /// Get all chunks of the table.
    ///
    /// This interface will be deprecated soon in this tutorial.
    pub fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let inner = self.inner.read().unwrap();
        Ok(inner.chunks.clone())
    }
}
