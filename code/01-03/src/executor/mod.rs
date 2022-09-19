//! Execute the queries.

use crate::binder::BoundStatement;
use crate::catalog::CatalogRef;

mod create;
mod select;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {}

/// Execute the bound AST.
pub struct Executor {
    catalog: CatalogRef,
}

impl Executor {
    /// Create a new executor.
    pub fn new(catalog: CatalogRef) -> Executor {
        Executor { catalog }
    }

    /// Execute a bound statement.
    pub fn execute(&self, stmt: BoundStatement) -> Result<String, ExecuteError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.execute_create_table(stmt),
            BoundStatement::Select(stmt) => self.execute_select(stmt),
        }
    }
}
