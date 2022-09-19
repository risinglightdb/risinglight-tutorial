use super::*;
use crate::array::ArrayImpl;
use crate::binder::BoundSelect;

impl Executor {
    pub fn execute_select(&self, stmt: BoundSelect) -> Result<DataChunk, ExecuteError> {
        let chunk = stmt.values.iter().map(ArrayImpl::from).collect();
        Ok(chunk)
    }
}
