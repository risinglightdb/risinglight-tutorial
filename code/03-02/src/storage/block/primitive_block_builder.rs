// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use super::super::encode::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

/// Encodes fixed-width data into a block. The layout is simply an array of
/// little endian fixed-width data.
pub struct PlainPrimitiveBlockBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    target_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            target_size,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockBuilder<T::ArrayType> for PlainPrimitiveBlockBuilder<T> {
    fn append(&mut self, item: Option<&T>) {
        item.expect("nullable item found in non-nullable block builder")
            .encode(&mut self.data);
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&T>) -> bool {
        !self.data.is_empty() && self.estimated_size() + T::WIDTH > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}
