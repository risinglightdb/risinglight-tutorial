// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use super::super::PrimitiveFixedWidthEncode;
use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder};

/// Scans one or several arrays from the block content.
pub struct PlainPrimitiveBlockIterator<T: PrimitiveFixedWidthEncode> {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockIterator<T> {
    pub fn new(block: Block, row_count: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType> for PlainPrimitiveBlockIterator<T> {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <T::ArrayType as Array>::Builder,
    ) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        let mut buffer = &self.block[self.next_row * T::WIDTH..];

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.next_row >= self.row_count {
                break;
            }

            builder.push(Some(&T::decode(&mut buffer)));
            cnt += 1;
            self.next_row += 1;
        }

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        self.next_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.next_row
    }
}
