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
        for chunk in table.all_chunks()? {
            yield chunk;
        }
    }
}
