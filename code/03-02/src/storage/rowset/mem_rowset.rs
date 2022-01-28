#![allow(dead_code)]

use std::sync::Arc;

use itertools::Itertools;

use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::ColumnCatalog;
use crate::storage::StorageResult;

pub struct MemRowset {
    builders: Vec<ArrayBuilderImpl>,
}

impl MemRowset {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ArrayBuilderImpl::with_capacity(0, column.desc().datatype()))
                .collect_vec(),
        }
    }

    fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        for (idx, column) in columns.arrays().iter().enumerate() {
            self.builders[idx].append(column);
        }
        Ok(())
    }

    fn flush(self) -> StorageResult<DataChunk> {
        Ok(self
            .builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }
}
