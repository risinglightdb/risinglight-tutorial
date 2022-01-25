//! Logical planner of `select` statement.

use super::*;
use crate::binder::{BoundExpr, BoundSelect};
use crate::types::{DataTypeExt, DataTypeKind};

impl LogicalPlanner {
    pub fn plan_select(&self, stmt: BoundSelect) -> Result<LogicalPlan, LogicalPlanError> {
        let plan: LogicalPlan = LogicalValues {
            column_types: stmt
                .values
                .iter()
                .map(|BoundExpr::Constant(v)| {
                    v.datatype()
                        .unwrap_or_else(|| DataTypeKind::Int(None).not_null())
                })
                .collect(),
            values: vec![stmt.values],
        }
        .into();
        Ok(plan)
    }
}
