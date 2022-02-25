//! Execute the queries.

use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;

use crate::array::DataChunk;
use crate::catalog::CatalogRef;
use crate::physical_planner::PhysicalPlan;
use crate::storage::{StorageError, StorageRef};

mod create;
mod dummy;
mod evaluator;
mod explain;
mod insert;
mod projection;
mod seq_scan;
mod values;

use self::create::*;
use self::dummy::*;
use self::explain::*;
use self::insert::*;
use self::projection::*;
use self::seq_scan::*;
use self::values::*;

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// A type-erased executor object.
///
/// Logically an executor is a stream of data chunks.
///
/// It consumes one or more streams from its child executors,
/// and produces a stream to its parent.
pub type BoxedExecutor = BoxStream<'static, Result<DataChunk, ExecuteError>>;

/// The builder of executor.
pub struct ExecutorBuilder {
    catalog: CatalogRef,
    storage: StorageRef,
    /// An optional runtime handle.
    ///
    /// If it is some, spawn the executor to runtime and return a channel receiver.
    handle: Option<tokio::runtime::Handle>,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(
        catalog: CatalogRef,
        storage: StorageRef,
        handle: Option<tokio::runtime::Handle>,
    ) -> ExecutorBuilder {
        ExecutorBuilder {
            catalog,
            storage,
            handle,
        }
    }

    /// Build executor from a [PhysicalPlan].
    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        use PhysicalPlan::*;
        let mut executor: BoxedExecutor = match plan {
            PhysicalCreateTable(plan) => CreateTableExecutor {
                plan,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
            }
            .execute(),
            PhysicalInsert(plan) => InsertExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
                child: self.build(*plan.child),
            }
            .execute(),
            PhysicalValues(plan) => ValuesExecutor {
                column_types: plan.column_types,
                values: plan.values,
            }
            .execute(),
            PhysicalExplain(plan) => ExplainExecutor { plan: plan.child }.execute(),
            PhysicalDummy(_) => DummyExecutor.execute(),
            PhysicalSeqScan(plan) => SeqScanExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids,
                storage: self.storage.clone(),
            }
            .execute(),
            PhysicalProjection(plan) => ProjectionExecutor {
                exprs: plan.exprs,
                child: self.build(*plan.child),
            }
            .execute(),
        };
        if let Some(handle) = &self.handle {
            // In parallel mode, we spawn the executor into the current tokio runtime,
            // connect it with a channel, and return the receiver as an executor.
            // Therefore, when used with tokio multi-thread runtime, they can run in parallel.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            handle.spawn(async move {
                while let Some(e) = executor.next().await {
                    tx.send(e).await.unwrap();
                }
            });
            tokio_stream::wrappers::ReceiverStream::new(rx).boxed()
        } else {
            executor
        }
    }
}
