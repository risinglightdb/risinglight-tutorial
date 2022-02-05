// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::DiskRowset;
use crate::array::{ArrayImpl, DataChunk};
use crate::storage::column::{ColumnIteratorImpl, ColumnSeekPosition};
use crate::storage::{StorageColumnRef, StorageResult};

/// When `expected_size` is not specified, we should limit the maximum size of the chunk.
const ROWSET_MAX_OUTPUT: usize = 65536;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    rowset: Arc<DiskRowset>,
    column_refs: Arc<[StorageColumnRef]>,
    column_iterators: Vec<Option<ColumnIteratorImpl>>,
}

impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        seek_pos: ColumnSeekPosition,
    ) -> StorageResult<Self> {
        let start_row_id = match seek_pos {
            ColumnSeekPosition::RowId(row_id) => row_id,
            _ => todo!(),
        };

        if column_refs.len() == 0 {
            panic!("no column to iterate")
        }

        let row_handler_count = column_refs
            .iter()
            .filter(|x| matches!(x, StorageColumnRef::RowHandler))
            .count();

        if row_handler_count > 1 {
            panic!("more than 1 row handler column")
        }

        if row_handler_count == column_refs.len() {
            panic!("no user column")
        }

        let mut column_iterators: Vec<Option<ColumnIteratorImpl>> = vec![];

        for column_ref in &*column_refs {
            // TODO: parallel seek
            match column_ref {
                StorageColumnRef::RowHandler => column_iterators.push(None),
                StorageColumnRef::Idx(idx) => column_iterators.push(Some(
                    ColumnIteratorImpl::new(
                        rowset.column(*idx as usize),
                        rowset.column_info(*idx as usize),
                        start_row_id,
                    )
                    .await?,
                )),
            };
        }

        Ok(Self {
            rowset,
            column_refs,
            column_iterators,
        })
    }

    /// Return (finished, data chunk of the current iteration)
    ///
    /// It is possible that after applying the deletion map, the current data chunk contains no
    /// element. In this case, the chunk will not be returned to the upper layer.
    ///
    /// TODO: check the deletion map before actually fetching data from column iterators.
    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<(bool, Option<DataChunk>)> {
        let fetch_size = if let Some(x) = expected_size {
            x
        } else {
            // When `expected_size` is not available, we try to dispatch
            // as little I/O as possible. We find the minimum fetch hints
            // from the column itertaors.
            let mut min = None;
            for it in self.column_iterators.iter().flatten() {
                let hint = it.fetch_hint();
                if hint != 0 {
                    if min.is_none() {
                        min = Some(hint);
                    } else {
                        min = Some(min.unwrap().min(hint));
                    }
                }
            }
            min.unwrap_or(ROWSET_MAX_OUTPUT)
        };

        let mut arrays: Vec<Option<ArrayImpl>> = vec![];
        let mut common_chunk_range = None;

        // TODO: parallel fetch
        // TODO: align unmatched rows

        // Fill column data
        for (id, column_ref) in self.column_refs.iter().enumerate() {
            match column_ref {
                StorageColumnRef::RowHandler => arrays.push(None),
                StorageColumnRef::Idx(_) => {
                    if let Some((row_id, array)) = self.column_iterators[id]
                        .as_mut()
                        .unwrap()
                        .next_batch(Some(fetch_size))
                        .await?
                    {
                        if let Some(x) = common_chunk_range {
                            if x != (row_id, array.len()) {
                                panic!("unmatched rowid from column iterator");
                            }
                        }
                        common_chunk_range = Some((row_id, array.len()));
                        arrays.push(Some(array));
                    } else {
                        arrays.push(None);
                    }
                }
            }
        }

        let _common_chunk_range = if let Some(common_chunk_range) = common_chunk_range {
            common_chunk_range
        } else {
            return Ok((true, None));
        };

        let chunk = DataChunk::from_iter(arrays.into_iter().map(Option::unwrap));

        Ok((
            false,
            if chunk.cardinality() > 0 {
                Some(chunk)
            } else {
                None
            },
        ))
    }

    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        loop {
            let (finished, batch) = self.next_batch_inner(expected_size).await?;
            if finished {
                return Ok(None);
            } else if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }
}
