//! On-disk storage

mod column;
mod rowset;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;

use self::rowset::{DiskRowset, RowSetBuilder};
use crate::array::DataChunk;
use crate::catalog::{ColumnDesc, TableRefId};

/// The error type of storage operations.
#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
pub struct StorageError(#[from] anyhow::Error);

/// A specialized `Result` type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub type StorageRef = Arc<DiskStorage>;
pub type StorageTableRef = Arc<DiskTable>;

/// On-disk storage.
pub struct DiskStorage {
    /// All tables in the current storage engine.
    tables: RwLock<HashMap<TableRefId, StorageTableRef>>,

    /// Generator for RowSet id.
    rowset_id_generator: Arc<AtomicU32>,

    /// The storage options.
    options: Arc<StorageOptions>,
}

pub struct StorageOptions {
    /// The directory of the storage
    pub base_path: PathBuf,
}

pub fn err(error: impl Into<anyhow::Error>) -> StorageError {
    StorageError(error.into())
}

/// An on-disk table.
pub struct DiskTable {
    /// Id of the table.
    id: TableRefId,

    /// Columns of the current table.
    column_descs: Arc<[ColumnDesc]>,

    /// The storage options.
    options: Arc<StorageOptions>,

    /// Generator for RowSet id.
    rowset_id_generator: Arc<AtomicU32>,

    /// RowSets in the table
    rowsets: RwLock<Vec<DiskRowset>>,
}

impl DiskStorage {
    /// Create a new in-memory storage.
    pub fn new(options: StorageOptions) -> Self {
        DiskStorage {
            tables: RwLock::new(HashMap::new()),
            options: Arc::new(options),
            rowset_id_generator: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Add a table.
    pub fn add_table(&self, id: TableRefId, column_descs: &[ColumnDesc]) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();
        let table = DiskTable {
            id,
            options: self.options.clone(),
            column_descs: column_descs.into(),
            rowsets: RwLock::new(Vec::new()),
            rowset_id_generator: self.rowset_id_generator.clone(),
        };
        let res = tables.insert(id, table.into());
        if res.is_some() {
            return Err(anyhow!("table already exists: {:?}", id).into());
        }
        Ok(())
    }

    /// Get a table.
    pub fn get_table(&self, id: TableRefId) -> StorageResult<StorageTableRef> {
        let tables = self.tables.read().unwrap();
        tables
            .get(&id)
            .ok_or_else(|| anyhow!("table not found: {:?}", id).into())
            .cloned()
    }
}

impl DiskTable {
    /// Start a transaction which only contains write.
    pub async fn write(self: &Arc<Self>) -> StorageResult<DiskTransaction> {
        let rowsets = self.rowsets.read().unwrap();
        Ok(DiskTransaction {
            read_only: false,
            table: self.clone(),
            rowset_snapshot: rowsets.clone(),
            builder: None,
            finished: false,
        })
    }

    /// Start a transaction which only contains read.
    pub async fn read(self: &Arc<Self>) -> StorageResult<DiskTransaction> {
        let rowsets = self.rowsets.read().unwrap();
        Ok(DiskTransaction {
            read_only: true,
            table: self.clone(),
            rowset_snapshot: rowsets.clone(),
            builder: None,
            finished: false,
        })
    }

    pub fn table_path(&self) -> PathBuf {
        self.options.base_path.join(self.id.table_id.to_string())
    }

    pub fn rowset_path_of(&self, rowset_id: u32) -> PathBuf {
        self.table_path().join(rowset_id.to_string())
    }
}

pub struct DiskTransaction {
    /// If this txn is read only.
    read_only: bool,

    /// Reference to table object
    table: Arc<DiskTable>,

    /// Current snapshot of RowSets
    rowset_snapshot: Vec<DiskRowset>,

    /// Builder for the RowSet
    builder: Option<RowSetBuilder>,

    /// Indicates whether the transaction is committed or aborted. If
    /// the [`SecondaryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,
}

impl Drop for DiskTransaction {
    fn drop(&mut self) {
        if !self.finished {
            warn!("Transaction dropped without committing or aborting");
        }
    }
}

impl DiskTransaction {
    /// Append a chunk to the table.
    pub async fn append(&mut self, chunk: DataChunk) -> StorageResult<()> {
        if self.read_only {
            return Err(anyhow!("cannot append chunks in read only txn!").into());
        }
        if self.builder.is_none() {
            self.builder = Some(RowSetBuilder::new(self.table.column_descs.clone()));
        }
        let builder = self.builder.as_mut().unwrap();

        builder.append(chunk)?;

        Ok(())
    }

    pub async fn commit(mut self) -> StorageResult<()> {
        self.finished = true;

        if let Some(builder) = self.builder.take() {
            use std::sync::atomic::Ordering::SeqCst;
            let rowset_id = self.table.rowset_id_generator.fetch_add(1, SeqCst);
            let rowset_path = self
                .table
                .options
                .base_path
                .join(self.table.rowset_path_of(rowset_id));
            let rowset = builder.flush(rowset_id, rowset_path).await?;
            let mut rowsets = self.table.rowsets.write().unwrap();
            rowsets.push(rowset);
        }

        Ok(())
    }

    /// Get all chunks of the table.
    pub async fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let mut chunks = vec![];
        for rowset in &self.rowset_snapshot {
            chunks.push(rowset.as_chunk().await?);
        }
        Ok(chunks)
    }
}
