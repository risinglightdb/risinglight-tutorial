//! Top-level structure of the database.

use std::sync::Arc;

use crate::binder::{BindError, Binder};
use crate::catalog::{CatalogRef, DatabaseCatalog};
use crate::executor::{ExecuteError, Executor};
use crate::parser::{parse, ParserError};

/// The database instance.
pub struct Database {
    catalog: CatalogRef,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new database instance.
    pub fn new() -> Self {
        let catalog = Arc::new(DatabaseCatalog::new());
        Database { catalog }
    }

    /// Run SQL queries and return the outputs.
    pub fn run(&self, sql: &str) -> Result<Vec<String>, Error> {
        // parse
        let stmts = parse(sql)?;
        let mut binder = Binder::new(self.catalog.clone());
        let executor = Executor::new(self.catalog.clone());

        let mut outputs = vec![];
        for stmt in stmts {
            let bound_stmt = binder.bind(&stmt)?;
            debug!("{:#?}", bound_stmt);
            let output = executor.execute(bound_stmt)?;
            outputs.push(output);
        }
        Ok(outputs)
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("bind error: {0}")]
    Bind(#[from] BindError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}
