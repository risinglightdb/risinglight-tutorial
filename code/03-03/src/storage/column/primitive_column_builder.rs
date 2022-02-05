// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::iter::Peekable;

use super::super::{BlockBuilder, PrimitiveFixedWidthEncode};
use super::{ColumnBuilder, ColumnBuilderOptions};
use crate::array::Array;
use crate::storage::proto::{BlockIndex, BlockType};
use crate::storage::{BlockIndexBuilder, BLOCK_HEADER_SIZE};

pub type I32ColumnBuilder = PrimitiveColumnBuilder<i32>;
pub type F64ColumnBuilder = PrimitiveColumnBuilder<f64>;
pub type BoolColumnBuilder = PrimitiveColumnBuilder<bool>;

/// Column builder of primitive types.
pub struct PrimitiveColumnBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,

    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<T::BlockBuilderType>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,
}

impl<T: PrimitiveFixedWidthEncode> PrimitiveColumnBuilder<T> {
    pub fn new(nullable: bool, options: ColumnBuilderOptions) -> Self {
        if nullable {
            panic!("nullable column is not supported yet");
        }
        Self {
            data: vec![],
            block_index_builder: BlockIndexBuilder::new(options.clone()),
            options,
            current_builder: None,
        }
    }

    fn finish_builder(&mut self) {
        let mut block_data = self.current_builder.take().unwrap().finish();

        self.block_index_builder.finish_block(
            BlockType::PrimitiveNonNull,
            &mut self.data,
            &mut block_data,
        );
    }
}

/// Append data to builder one by one. After appending each item, check if
/// the block should be finished. Return true if a new block builder should
/// be created.
///
/// In the future, for integer data, we should be able to skip the `should_finish`
/// check, as we can calculate expected number of items to add simply by
/// `size_of::<T>() * N`.
pub fn append_one_by_one<'a, A: Array>(
    iter: &mut Peekable<impl Iterator<Item = Option<&'a A::Item>>>,
    builder: &mut impl BlockBuilder<A>,
) -> (usize, bool) {
    let mut cnt = 0;
    while let Some(to_be_appended) = iter.peek() {
        // peek and see if we could push more items into the builder

        if builder.should_finish(to_be_appended) {
            return (cnt, true);
        }

        // get the item from iterator and push it to the builder
        let to_be_appended = iter.next().unwrap();

        builder.append(to_be_appended);
        cnt += 1;
    }

    (cnt, false)
}

impl<T: PrimitiveFixedWidthEncode> ColumnBuilder<T::ArrayType> for PrimitiveColumnBuilder<T> {
    fn append(&mut self, array: &T::ArrayType) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                self.current_builder =
                    Some(<T::BlockBuilderType as BlockBuilder<T::ArrayType>>::new(
                        self.options.target_block_size - BLOCK_HEADER_SIZE,
                    ));
            }

            let builder = self.current_builder.as_mut().unwrap();

            let (row_count, should_finish) = append_one_by_one(&mut iter, builder);

            self.block_index_builder.add_rows(row_count);

            // finish the current block
            if should_finish {
                self.finish_builder();
            }
        }
    }

    fn finish(mut self) -> (Vec<BlockIndex>, Vec<u8>) {
        self.finish_builder();

        (self.block_index_builder.into_index(), self.data)
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;
    use crate::array::I32Array;

    #[test]
    fn test_i32_column_builder_finish_boundary() {
        let item_each_block = (128 - 16) / 4;
        // In the first case, we append array that just fits size of each block
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..10 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(item_each_block),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
        assert_eq!(index[3].first_rowid as usize, item_each_block * 3);
        assert_eq!(index[3].row_count as usize, item_each_block);

        // In this case, we append array that is smaller than each block, and fill fewer than 2
        // blocks of contents
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..12 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(4),
            ));
        }
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append two array that sums up to exactly 2 blocks
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(30),
        ));
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(26),
        ));
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append an array that is larger than 1 block.
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        builder.append(&I32Array::from_iter(
            [Some(1)]
                .iter()
                .cycle()
                .cloned()
                .take(item_each_block * 100),
        ));
        assert_eq!(builder.finish().0.len(), 100);

        // And finally, some chaos test
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        assert_eq!(builder.finish().0.len(), 83);
    }

    #[test]
    fn test_nullable_i32_column_builder() {
        let mut builder =
            I32ColumnBuilder::new(true, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        builder.finish();
    }
}