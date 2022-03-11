//! Execute the queries.

use crate::array::DataChunk;
use crate::binder::BoundStatement;
use crate::catalog::CatalogRef;
use crate::storage::{StorageError, StorageRef};

mod create;
mod select;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Execute the bound AST.
pub struct Executor {
    catalog: CatalogRef,
    storage: StorageRef,
}

impl Executor {
    /// Create a new executor.
    pub fn new(catalog: CatalogRef, storage: StorageRef) -> Executor {
        Executor { catalog, storage }
    }

    /// Execute a bound statement.
    pub fn execute(&self, stmt: BoundStatement) -> Result<DataChunk, ExecuteError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.execute_create_table(stmt),
            BoundStatement::Select(stmt) => self.execute_select(stmt),
        }
    }
}
