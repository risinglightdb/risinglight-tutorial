use anyhow::anyhow;
use bytes::{Buf, BufMut};

use super::StorageResult;
use crate::array::{Array, ArrayBuilder, I32Array, I32ArrayBuilder};

/// Encode an `I32Array` into a `Vec<u8>`.
pub fn encode_int32_column(a: &I32Array, mut buffer: impl BufMut) -> StorageResult<()> {
    for item in a.iter() {
        if let Some(item) = item {
            buffer.put_i32_le(*item);
        } else {
            return Err(anyhow!("nullable encoding not supported!").into());
        }
    }
    Ok(())
}

pub fn decode_int32_column(mut data: impl Buf) -> StorageResult<I32Array> {
    let mut builder = I32ArrayBuilder::with_capacity(data.remaining() / 4);
    while data.has_remaining() {
        builder.push(Some(&data.get_i32_le()));
    }
    Ok(builder.finish())
}
