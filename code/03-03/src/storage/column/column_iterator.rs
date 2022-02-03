// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::{Column, ColumnIterator, ConcreteColumnIterator};
use crate::array::{Array, ArrayImpl, BoolArray, F64Array, I32Array};
use crate::catalog::ColumnCatalog;
use crate::storage::{PlainPrimitiveBlockIterator, StorageResult};
use crate::types::DataTypeKind;

pub type I32ColumnIterator = ConcreteColumnIterator<I32Array, PlainPrimitiveBlockIterator<i32>>;
pub type F64ColumnIterator = ConcreteColumnIterator<F64Array, PlainPrimitiveBlockIterator<f64>>;
pub type BoolColumnIterator = ConcreteColumnIterator<BoolArray, PlainPrimitiveBlockIterator<bool>>;

/// [`ColumnIteratorImpl`] of all types
pub enum ColumnIteratorImpl {
    Int32(I32ColumnIterator),
    Float64(F64ColumnIterator),
    Bool(BoolColumnIterator),
}

impl ColumnIteratorImpl {
    pub async fn new(
        column: Column,
        column_info: &ColumnCatalog,
        start_pos: u32,
    ) -> StorageResult<Self> {
        let iter = match column_info.datatype().kind() {
            DataTypeKind::Int(_) => Self::Int32(I32ColumnIterator::new(column, start_pos).await?),
            DataTypeKind::Boolean => Self::Bool(BoolColumnIterator::new(column, start_pos).await?),
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(F64ColumnIterator::new(column, start_pos).await?)
            }
            other_datatype => todo!(
                "column iterator for {:?} is not implemented",
                other_datatype
            ),
        };
        Ok(iter)
    }

    
    fn erase_concrete_type(
        ret: Option<(u32, impl Array + Into<ArrayImpl>)>,
    ) -> Option<(u32, ArrayImpl)> {
        ret.map(|(row_id, array)| (row_id, array.into()))
    }

    
    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<(u32, ArrayImpl)>> {
        let result = match self {
            Self::Int32(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Float64(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Bool(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
        };
        Ok(result)
    }

    
    pub fn fetch_hint(&self) -> usize {
        match self {
            Self::Int32(it) => it.fetch_hint(),
            Self::Float64(it) => it.fetch_hint(),
            Self::Bool(it) => it.fetch_hint(),
        }
    }
}
