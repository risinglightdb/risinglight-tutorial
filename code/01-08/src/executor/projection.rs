use super::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub exprs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let chunk = self
                .exprs
                .iter()
                .map(|expr| expr.eval_array(&batch))
                .collect::<Result<DataChunk, _>>()?;
            yield chunk;
        }
    }
}
