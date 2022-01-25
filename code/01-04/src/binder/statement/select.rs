use super::*;
use crate::parser::{Expr, Query, SelectItem, SetExpr};
use crate::types::DataValue;

/// A bound `SELECT` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub values: Vec<DataValue>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        match &query.body {
            SetExpr::Select(select) => {
                let mut values = vec![];
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(Expr::Value(v)) => values.push(v.into()),
                        _ => todo!("not supported statement: {:#?}", query),
                    }
                }
                Ok(BoundSelect { values })
            }
            _ => todo!("not supported statement: {:#?}", query),
        }
    }
}
