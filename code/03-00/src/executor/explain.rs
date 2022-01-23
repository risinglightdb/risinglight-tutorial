use super::*;
use crate::array::ArrayImpl;
use crate::physical_planner::PhysicalPlan;

/// The executor of `EXPLAIN` statement.
pub struct ExplainExecutor {
    pub plan: Box<PhysicalPlan>,
}

impl ExplainExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        let explain_result = format!("{}", *self.plan);
        let chunk = DataChunk::from_iter([ArrayImpl::Utf8(
            [Some(explain_result)].into_iter().collect(),
        )]);
        yield chunk;
    }
}
