use super::*;
use crate::array::ArrayImpl;
use crate::binder::{BoundExpr, BoundSelect};

/// The executor of `SELECT` statement.
pub struct SelectExecutor {
    pub stmt: BoundSelect,
}

impl Executor for SelectExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let chunk = (self.stmt.values.iter())
            .map(|BoundExpr::Constant(v)| ArrayImpl::from(v))
            .collect();
        Ok(chunk)
    }
}
