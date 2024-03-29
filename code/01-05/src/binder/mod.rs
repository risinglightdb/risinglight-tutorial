//! Resolve all expressions referring with their names.

use std::vec::Vec;

use crate::catalog::*;
use crate::parser::{Ident, ObjectName, Statement};

mod expression;
mod statement;

pub use self::expression::*;
pub use self::statement::*;

/// A bound SQL statement generated by the [`Binder`].
#[derive(Debug, PartialEq, Clone)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Insert(BoundInsert),
    Select(BoundSelect),
}

/// The error type of bind operations.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("table must have at least one column")]
    EmptyColumns,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("duplicated table: {0}")]
    DuplicatedTable(String),
    #[error("duplicated column: {0}")]
    DuplicatedColumn(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("not nullable column: {0}")]
    NotNullableColumn(String),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    TupleLengthMismatch { expected: usize, actual: usize },
    #[error("value should not be null in column: {0}")]
    NullValueInColumn(String),
}

/// The binder resolves all expressions referring to schema objects such as
/// tables or views with their column names and types.
pub struct Binder {
    catalog: CatalogRef,
}

impl Binder {
    /// Create a new [Binder].
    pub fn new(catalog: CatalogRef) -> Self {
        Binder { catalog }
    }

    /// Bind a statement.
    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        use Statement::*;
        match stmt {
            CreateTable { .. } => Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?)),
            Insert { .. } => Ok(BoundStatement::Insert(self.bind_insert(stmt)?)),
            Query(query) => Ok(BoundStatement::Select(self.bind_select(query)?)),
            _ => todo!("bind statement: {:#?}", stmt),
        }
    }
}

/// Split an [ObjectName] into `(schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<(&str, &str), BindError> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}
