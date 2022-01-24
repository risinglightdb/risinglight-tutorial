//! In-memory storage.
//!
//! RisingLight's in-memory representation of data is very simple. Currently,
//! it is simple a vector of `DataChunk`. Upon insertion, users' data are
//! simply appended to the end of the vector.

mod rowset;
mod table_transaction;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use self::table_transaction::TableTransaction;
use crate::array::DataChunk;
use crate::catalog::TableRefId;

/// The error type of storage operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("table not found: {0:?}")]
    NotFound(TableRefId),
}

/// A specialized `Result` type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub type StorageRef = Arc<DiskStorage>;
pub type DiskTableRef = Arc<DiskTable>;

/// In-memory storage.
pub struct DiskStorage {
    tables: Mutex<HashMap<TableRefId, DiskTableRef>>,
}

impl Default for DiskStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskStorage {
    /// Create a new in-memory storage.
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
            .ok_or(StorageError::NotFound(id))
    }
}

/// A table in in-memory engine.
pub struct DiskTable {
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    async fn write(self: &Arc<Self>) -> StorageResult<TableTransaction> {
        Ok(TableTransaction::start(self.clone(), false, false).await?)
    }

    #[allow(dead_code)]
    async fn read(self: &Arc<Self>) -> StorageResult<TableTransaction> {
        Ok(TableTransaction::start(self.clone(), true, false).await?)
    }

    #[allow(dead_code)]
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
