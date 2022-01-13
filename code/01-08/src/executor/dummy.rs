use super::*;

/// A dummy executor that produces a single value.
pub struct DummyExecutor;

impl DummyExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        yield DataChunk::single(0);
    }
}
