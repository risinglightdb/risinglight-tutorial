use super::*;
use crate::array::DataChunk;
use crate::catalog::{ColumnId, TableRefId};

/// The executor of sequential scan operation.
pub struct SeqScanExecutor {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: StorageRef,
}

impl SeqScanExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        let table = self.storage.get_table(self.table_ref_id)?;
        let txn = table.read().await?;

        for chunk in txn.all_chunks().await? {
            yield chunk;
        }

        txn.commit().await?;
    }
}
