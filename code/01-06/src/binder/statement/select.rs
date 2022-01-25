use super::*;
use crate::parser::{Query, SelectItem, SetExpr};

/// A bound `SELECT` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub values: Vec<BoundExpr>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        match &query.body {
            SetExpr::Select(select) => {
                let mut values = vec![];
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => values.push(self.bind_expr(expr)?),
                        _ => todo!("not supported statement: {:#?}", query),
                    }
                }
                return Ok(BoundSelect { values });
            }
            _ => todo!("not supported statement: {:#?}", query),
        }
    }
}
