use std::sync::Arc;

use tempfile::tempdir;

use super::*;
use crate::array::{Array, ArrayImpl, DataChunk, F64Array, I32Array};
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::storage::{ColumnBuilderOptions, ColumnSeekPosition, StorageColumnRef};
use crate::types::{DataType, DataTypeKind};

fn column_desc() -> (ColumnCatalog, ColumnCatalog) {
    let col1 = ColumnCatalog::new(
        0,
        "test1".into(),
        ColumnDesc::new(DataType::new(DataTypeKind::Int(None), false), false),
    );
    let col2 = ColumnCatalog::new(
        1,
        "test2".into(),
        ColumnDesc::new(DataType::new(DataTypeKind::Float(None), false), false),
    );
    (col1, col2)
}

fn helper_build_data_chunk() -> DataChunk {
    let (col1, col2) = column_desc();
    let mut mem_rowset = MemRowset::new(vec![col1, col2].into());
    for i in 0..1000 {
        let col1_data: ArrayImpl = I32Array::from_iter(i * 100..(i + 1) * 100).into();
        let col2_data: ArrayImpl =
            F64Array::from_iter((i * 100..(i + 1) * 100).map(|x| x as f64)).into();
        mem_rowset
            .append(DataChunk::from_iter(vec![col1_data, col2_data].into_iter()))
            .unwrap();
    }
    mem_rowset.flush().unwrap()
}

#[tokio::test]
async fn test_rowset_build_and_iterate() {
    let tempdir = tempdir().unwrap();
    let rowset_path = tempdir.path().join("test_rowset_1");
    tokio::fs::create_dir(&rowset_path).await.unwrap();
    let chunk = helper_build_data_chunk();
    let (col1, col2) = column_desc();
    let desc: Arc<[ColumnCatalog]> = vec![col1, col2].into();
    let mut builder = RowsetBuilder::new(
        desc.clone(),
        &rowset_path,
        ColumnBuilderOptions::default_for_column_test(),
    );
    builder.append(chunk);
    builder.finish_and_flush().await.unwrap();
    let disk_rowset = DiskRowset::open(rowset_path, desc, 0).await.unwrap();
    let disk_rowset = Arc::new(disk_rowset);
    let mut iter = RowSetIterator::new(
        disk_rowset,
        vec![StorageColumnRef::Idx(0), StorageColumnRef::Idx(1)].into(),
        ColumnSeekPosition::start(),
    )
    .await
    .unwrap();
    let batch_size = 103;
    let mut cnt = 0;
    while let Some(batch) = iter.next_batch(Some(batch_size)).await.unwrap() {
        let array1: &I32Array = (&batch.arrays()[0]).try_into().unwrap();
        let array2: &F64Array = (&batch.arrays()[1]).try_into().unwrap();
        assert_eq!(array1.len(), array2.len());
        for (idx, item) in array1.iter().enumerate() {
            assert_eq!(*item.unwrap(), (idx + cnt) as i32);
        }
        for (idx, item) in array2.iter().enumerate() {
            assert_eq!(*item.unwrap(), (idx + cnt) as f64);
        }
        cnt += array1.len();
    }
    assert_eq!(cnt, 100 * 1000);
}
