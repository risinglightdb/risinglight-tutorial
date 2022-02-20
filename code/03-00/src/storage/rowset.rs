use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;

use super::column::{decode_int32_column, encode_int32_column};
use super::{err, StorageResult};
use crate::array::{ArrayImpl, DataChunk};
use crate::catalog::ColumnDesc;

fn column_path(rowset_path: impl AsRef<Path>, column_id: usize) -> PathBuf {
    rowset_path.as_ref().join(format!("{}.col", column_id))
}

#[derive(Clone)]
pub struct DiskRowset {
    /// Columns of the current RowSet.
    column_descs: Arc<[ColumnDesc]>,

    /// Id of the current rowset within the table.
    #[allow(dead_code)]
    rowset_id: u32,

    /// Base path of the RowSet
    rowset_path: PathBuf,
}

impl DiskRowset {
    pub async fn as_chunk(&self) -> StorageResult<DataChunk> {
        let mut columns = vec![];
        for (idx, _) in self.column_descs.iter().enumerate() {
            let column_path = column_path(&self.rowset_path, idx);
            let data = tokio::fs::read(column_path).await.map_err(err)?;
            columns.push(decode_int32_column(&data[..])?);
        }
        Ok(columns.into_iter().map(ArrayImpl::Int32).collect())
    }
}

pub struct RowSetBuilder {
    /// Columns of the current RowSet.
    column_descs: Arc<[ColumnDesc]>,

    /// Buffer of all column data
    buffer: Vec<Vec<u8>>,
}

impl RowSetBuilder {
    pub fn new(column_descs: Arc<[ColumnDesc]>) -> Self {
        RowSetBuilder {
            buffer: (0..column_descs.len()).map(|_| vec![]).collect_vec(),
            column_descs,
        }
    }

    pub fn append(&mut self, chunk: DataChunk) -> StorageResult<()> {
        for (idx, column) in chunk.arrays().iter().enumerate() {
            if let ArrayImpl::Int32(column) = column {
                encode_int32_column(column, &mut self.buffer[idx])?;
            } else {
                return Err(anyhow!("unsupported column type").into());
            }
        }
        Ok(())
    }

    pub async fn flush(
        self,
        rowset_id: u32,
        rowset_path: impl AsRef<Path>,
    ) -> StorageResult<DiskRowset> {
        let rowset_path = rowset_path.as_ref();

        tokio::fs::create_dir_all(rowset_path).await.map_err(err)?;

        for (idx, _) in self.column_descs.iter().enumerate() {
            let column_path = column_path(rowset_path, idx);
            tokio::fs::write(column_path, &self.buffer[idx])
                .await
                .map_err(err)?;
        }

        Ok(DiskRowset {
            column_descs: self.column_descs,
            rowset_id,
            rowset_path: rowset_path.into(),
        })
    }
}
