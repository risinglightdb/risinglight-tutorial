use super::*;
use crate::binder::BoundCreateTable;
use crate::catalog::TableRefId;

impl Executor {
    pub fn execute_create_table(&self, stmt: BoundCreateTable) -> Result<DataChunk, ExecuteError> {
        let schema = self.catalog.get_schema(stmt.schema_id).unwrap();
        let table_id = schema.add_table(&stmt.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        for (name, desc) in &stmt.columns {
            table.add_column(name, desc.clone()).unwrap();
        }
        self.storage
            .add_table(TableRefId::new(stmt.schema_id, table_id))?;
        Ok(DataChunk::single(1))
    }
}
