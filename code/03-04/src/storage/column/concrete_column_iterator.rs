// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use async_trait::async_trait;

use super::super::{Block, BlockIterator};
use super::{Column, ColumnIterator, ColumnSeekPosition};
use crate::array::{Array, ArrayBuilder};
use crate::storage::{BlockIndex, BlockType, StorageResult};

/// Column iterator that operates on a concrete type
pub struct ConcreteColumnIterator<A: Array, BI: BlockIterator<A>> {
    /// The [`Column`] object to iterate.
    column: Column,

    /// ID of the current block.
    current_block_id: u32,

    /// Block iterator.
    block_iterator: BI,

    /// RowID of the current column.
    current_row_id: u32,

    /// Indicates whether this iterator has finished or not.
    finished: bool,

    /// `A` doesn't appear in struct, so we need a phantom marker for that.
    _phantom: PhantomData<A>,
}

fn get_iterator_for<A: Array, F: BlockIterator<A>>(
    _block_type: BlockType,
    block: Block,
    index: &BlockIndex,
    start_pos: usize,
) -> F {
    // TODO: dispatch to the correct block iterator using `block_type`. Currently, we always use the
    // type generic parameter `F` to choose the iterator to use. In the future, if we have other
    // encoding schemes, like RLE encoding, we will need to use the `block_type` parameter.
    let mut it = F::new(block, index.row_count as usize);
    it.skip(start_pos - index.first_rowid as usize);
    it
}

impl<A: Array, F: BlockIterator<A>> ConcreteColumnIterator<A, F> {
    pub async fn new(column: Column, start_pos: u32) -> StorageResult<Self> {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let (header, block) = column.get_block(current_block_id).await?;
        Ok(Self {
            block_iterator: get_iterator_for(
                header.block_type,
                block,
                column.index().index(current_block_id),
                start_pos as usize,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            _phantom: PhantomData,
        })
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<(u32, A)>> {
        if self.finished {
            return Ok(None);
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            self.block_iterator.remaining_items()
        };

        let mut builder = A::Builder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        loop {
            let cnt = self
                .block_iterator
                .next_batch(expected_size.map(|x| x - total_cnt), &mut builder);

            total_cnt += cnt;
            self.current_row_id += cnt as u32;

            if let Some(expected_size) = expected_size {
                if total_cnt >= expected_size {
                    break;
                }
            } else if total_cnt != 0 {
                break;
            }

            self.current_block_id += 1;

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                break;
            }

            let (header, block) = self.column.get_block(self.current_block_id).await?;
            self.block_iterator = get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
            );
        }

        if total_cnt == 0 {
            Ok(None)
        } else {
            Ok(Some((first_row_id, builder.finish())))
        }
    }

    fn fetch_hint_inner(&self) -> usize {
        if self.finished {
            return 0;
        }
        let index = self.column.index().index(self.current_block_id);
        (index.row_count - (self.current_row_id - index.first_rowid)) as usize
    }
}

#[async_trait]
impl<A: Array, F: BlockIterator<A>> ColumnIterator<A> for ConcreteColumnIterator<A, F> {
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<(u32, A)>> {
        self.next_batch_inner(expected_size).await
    }

    fn fetch_hint(&self) -> usize {
        self.fetch_hint_inner()
    }
}
