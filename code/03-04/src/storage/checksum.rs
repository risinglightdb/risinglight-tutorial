// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use anyhow::anyhow;

use super::{ChecksumType, StorageError};
use crate::storage::StorageResult;

pub fn build_checksum(checksum_type: ChecksumType, block_data: &[u8]) -> u64 {
    match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Crc32 => crc32fast::hash(block_data) as u64,
    }
}


pub fn verify_checksum(
    checksum_type: ChecksumType,
    index_data: &[u8],
    checksum: u64,
) -> StorageResult<()> {
    let chksum = match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Crc32 => crc32fast::hash(index_data) as u64,
    };
    if chksum != checksum {
        return Err(StorageError(anyhow!(
            "checksum mismatch: found {}, expected {}",
            chksum,
            checksum
        )));
    }
    Ok(())
}
