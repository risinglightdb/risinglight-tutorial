//! Top-level structure of the database.

use std::sync::Arc;

use futures::TryStreamExt;
use tokio::runtime::Runtime;

use crate::array::DataChunk;
use crate::binder::{BindError, Binder};
use crate::catalog::{CatalogRef, DatabaseCatalog};
use crate::executor::{ExecuteError, ExecutorBuilder};
use crate::logical_planner::{LogicalPlanError, LogicalPlanner};
use crate::parser::{parse, ParserError};
use crate::physical_planner::{PhysicalPlanError, PhysicalPlanner};
use crate::storage::{DiskStorage, StorageOptions};

/// The database instance.
pub struct Database {
    catalog: CatalogRef,
    executor_builder: ExecutorBuilder,
    runtime: Runtime,
}

impl Database {
    /// Create a new database instance.
    pub fn new(options: StorageOptions) -> Self {
        let catalog = Arc::new(DatabaseCatalog::new());
        let storage = Arc::new(DiskStorage::new(options));
        let parallel = matches!(std::env::var("LIGHT_PARALLEL"), Ok(s) if s == "1");
        let runtime = if parallel {
            tokio::runtime::Builder::new_multi_thread()
        } else {
            tokio::runtime::Builder::new_current_thread()
        }
        .build()
        .expect("failed to create tokio runtime");
        let handle = parallel.then(|| runtime.handle().clone());
        Database {
            catalog: catalog.clone(),
            executor_builder: ExecutorBuilder::new(catalog, storage, handle),
            runtime,
        }
    }

    /// Run SQL queries and return the outputs.
    pub fn run(&self, sql: &str) -> Result<Vec<DataChunk>, Error> {
        // parse
        let stmts = parse(sql)?;

        let mut outputs = vec![];
        for stmt in stmts {
            let mut binder = Binder::new(self.catalog.clone());
            let logical_planner = LogicalPlanner::default();
            let physical_planner = PhysicalPlanner::default();

            let bound_stmt = binder.bind(&stmt)?;
            debug!("{:#?}", bound_stmt);
            let logical_plan = logical_planner.plan(bound_stmt)?;
            debug!("{:#?}", logical_plan);
            let physical_plan = physical_planner.plan(&logical_plan)?;
            debug!("{:#?}", physical_plan);
            let mut executor = self.executor_builder.build(physical_plan);
            self.runtime.block_on(async {
                while let Some(chunk) = executor.try_next().await? {
                    outputs.push(chunk);
                }
                Ok(()) as Result<(), Error>
            })?;
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
    #[error("logical plan error: {0}")]
    LogicalPlan(#[from] LogicalPlanError),
    #[error("physical plan error: {0}")]
    PhysicalPlan(#[from] PhysicalPlanError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}
