// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::sync::Arc;

use itertools::Itertools;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

use super::{path_of_data_column, path_of_index_column, RowSetIterator};
use crate::catalog::ColumnCatalog;
use crate::storage::column::{Column, ColumnSeekPosition};
use crate::storage::{ColumnIndex, StorageColumnRef, StorageResult};

/// Represents a column in Secondary.
///
/// [`DiskRowset`] contains all necessary information, e.g. column info, rowset location.
pub struct DiskRowset {
    column_infos: Arc<[ColumnCatalog]>,
    columns: Vec<Column>,
    rowset_id: u32,
}

impl DiskRowset {
    pub async fn open(
        directory: PathBuf,
        column_infos: Arc<[ColumnCatalog]>,
        rowset_id: u32,
    ) -> StorageResult<Self> {
        let mut columns = vec![];

        for column_info in column_infos.iter() {
            let file = OpenOptions::default()
                .read(true)
                .write(false)
                .open(path_of_data_column(&directory, column_info))
                .await?;

            let mut index = OpenOptions::default()
                .read(true)
                .write(false)
                .open(path_of_index_column(&directory, column_info))
                .await?;

            // TODO(chi): add an index cache later
            let mut index_content = vec![];
            index.read_to_end(&mut index_content).await?;

            let column = Column::new(
                ColumnIndex::from_bytes(&index_content)?,
                Arc::new(file.into_std().await),
            );
            columns.push(column);
        }

        Ok(Self {
            column_infos,
            columns,
            rowset_id,
        })
    }

    pub fn column(&self, storage_column_id: usize) -> Column {
        self.columns[storage_column_id].clone()
    }

    pub fn column_info(&self, storage_column_id: usize) -> &ColumnCatalog {
        &self.column_infos[storage_column_id]
    }

    pub fn rowset_id(&self) -> u32 {
        self.rowset_id
    }

    pub async fn iter(
        self: &Arc<Self>,
        column_refs: Arc<[StorageColumnRef]>,
        seek_pos: ColumnSeekPosition,
    ) -> StorageResult<RowSetIterator> {
        RowSetIterator::new(self.clone(), column_refs, seek_pos).await
    }

    pub fn on_disk_size(&self) -> u64 {
        self.columns
            .iter()
            .map(|x| x.on_disk_size())
            .sum1()
            .unwrap_or(0)
    }
}
