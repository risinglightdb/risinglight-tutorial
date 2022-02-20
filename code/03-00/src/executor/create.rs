use super::*;
use crate::catalog::TableRefId;
use crate::physical_planner::PhysicalCreateTable;

/// The executor of `CREATE TABLE` statement.
pub struct CreateTableExecutor {
    pub plan: PhysicalCreateTable,
    pub catalog: CatalogRef,
    pub storage: StorageRef,
}

impl CreateTableExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        let schema = self.catalog.get_schema(self.plan.schema_id).unwrap();
        let table_id = schema.add_table(&self.plan.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        let mut column_descs = vec![];
        for (name, desc) in &self.plan.columns {
            table.add_column(name, desc.clone()).unwrap();
            column_descs.push(desc.clone());
        }
        self.storage.add_table(
            TableRefId::new(self.plan.schema_id, table_id),
            &column_descs,
        )?;
        yield DataChunk::single(1);
    }
}
