use super::*;
use crate::array::ArrayImpl;
use crate::binder::{BoundExpr, BoundSelect};

impl Executor {
    pub fn execute_select(&self, stmt: BoundSelect) -> Result<DataChunk, ExecuteError> {
        let chunk = (stmt.values.iter())
            .map(|BoundExpr::Constant(v)| ArrayImpl::from(v))
            .collect();
        Ok(chunk)
    }
}
