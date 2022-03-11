use super::*;
use crate::binder::BoundSelect;
use crate::parser::Value;

impl Executor {
    pub fn execute_select(&self, stmt: BoundSelect) -> Result<String, ExecuteError> {
        let mut output = String::new();
        for v in &stmt.values {
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
