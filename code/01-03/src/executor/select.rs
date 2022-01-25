use super::*;
use crate::binder::BoundSelect;
use crate::parser::Value;

/// The executor of `SELECT` statement.
pub struct SelectExecutor {
    pub stmt: BoundSelect,
}

impl Executor for SelectExecutor {
    fn execute(&mut self) -> Result<String, ExecuteError> {
        let mut output = String::new();
        for v in &self.stmt.values {
            output += " ";
            match v {
                Value::SingleQuotedString(s) => output += s,
                Value::Number(s, _) => output += s,
                _ => todo!("not supported value: {:#?}", v),
            }
        }
        Ok(output.trim().to_string())
    }
}
