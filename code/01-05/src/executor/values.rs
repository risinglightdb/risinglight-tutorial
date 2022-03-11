use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::{BoundExpr, BoundInsert};
use crate::types::DataValue;

impl Executor {
    pub fn execute_values(&self, stmt: BoundInsert) -> Result<DataChunk, ExecuteError> {
        let cardinality = stmt.values.len();
        let mut builders = stmt
            .column_types
            .iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(cardinality, ty))
            .collect_vec();
        for row in &stmt.values {
            for (expr, builder) in row.iter().zip(&mut builders) {
                let value = expr.eval_const()?;
                builder.push(&value);
            }
        }
        let chunk = builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>();
        Ok(chunk)
    }
}

impl BoundExpr {
    /// Evaluate the constant expression.
    pub fn eval_const(&self) -> Result<DataValue, ExecuteError> {
        match self {
            Self::Constant(v) => Ok(v.clone()),
        }
    }
}
