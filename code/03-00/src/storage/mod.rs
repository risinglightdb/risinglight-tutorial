//! On-disk storage

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use bytes::{Buf, BufMut};

use crate::array::{Array, ArrayBuilder, ArrayImpl, DataChunk, I32Array, I32ArrayBuilder};
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

    /// The storage options.
    options: Arc<StorageOptions>,
}

pub struct StorageOptions {
    /// The directory of the storage
    base_path: PathBuf,
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
            tables: RwLock::new(HashMap::new()),
            options: Arc::new(StorageOptions {
                base_path: "risinglight.db".into(),
            }),
        }
    }

    /// Add a table.
    pub fn add_table(&self, id: TableRefId, column_descs: &[ColumnDesc]) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();
        let table = DiskTable {
            id,
            options: self.options.clone(),
            column_descs: column_descs.into(),
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

/// Encode an `I32Array` into a `Vec<u8>`.
fn encode_int32_column(a: &I32Array) -> StorageResult<Vec<u8>> {
    let mut buffer = Vec::with_capacity(a.len() * 4);
    for item in a.iter() {
        if let Some(item) = item {
            buffer.put_i32_le(*item);
        } else {
            return Err(anyhow!("nullable encoding not supported!").into());
        }
    }
    Ok(buffer)
}

fn decode_int32_column(mut data: &[u8]) -> StorageResult<I32Array> {
    let mut builder = I32ArrayBuilder::with_capacity(data.len() / 4);
    while data.has_remaining() {
        builder.push(Some(&data.get_i32_le()));
    }
    Ok(builder.finish())
}

impl DiskTable {
    fn table_path(&self) -> PathBuf {
        self.options.base_path.join(self.id.table_id.to_string())
    }

    fn column_path(&self, column_id: usize) -> PathBuf {
        self.table_path().join(format!("{}.col", column_id))
    }

    /// Append a chunk to the table.
    pub async fn append(&self, chunk: DataChunk) -> StorageResult<()> {
        for (idx, column) in chunk.arrays().iter().enumerate() {
            if let ArrayImpl::Int32(column) = column {
                let column_path = self.column_path(idx);
                let data = encode_int32_column(column)?;
                tokio::fs::create_dir_all(column_path.parent().unwrap())
                    .await
                    .map_err(err)?;
                tokio::fs::write(column_path, data).await.map_err(err)?;
            } else {
                return Err(anyhow!("unsupported column type").into());
            }
        }
        Ok(())
    }

    /// Get all chunks of the table.
    pub async fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let mut columns = vec![];
        for (idx, _) in self.column_descs.iter().enumerate() {
            let column_path = self.column_path(idx);
            let data = tokio::fs::read(column_path).await.map_err(err)?;
            columns.push(decode_int32_column(&data)?);
        }
        Ok(vec![columns.into_iter().map(ArrayImpl::Int32).collect()])
    }
}
