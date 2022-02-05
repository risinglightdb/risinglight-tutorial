use super::*;
use crate::array::{ArrayImpl, DataChunk, F64Array, I32Array};
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::types::{DataType, DataTypeKind};

#[tokio::test]
async fn test_mem_rowset_build() {
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
    let mut mem_rowset = MemRowset::new(vec![col1, col2].into());
    let col1_data: ArrayImpl = I32Array::from_iter(vec![1, 2, 3, 4, 5]).into();
    let col2_data: ArrayImpl = F64Array::from_iter(vec![1.0, 2.0, 3.0, 4.0, 5.0]).into();
    mem_rowset
        .append(DataChunk::from_iter(
            vec![col1_data.clone(), col2_data.clone()].into_iter(),
        ))
        .unwrap();
    mem_rowset
        .append(DataChunk::from_iter(
            vec![col1_data.clone(), col2_data.clone()].into_iter(),
        ))
        .unwrap();
    mem_rowset
        .append(DataChunk::from_iter(vec![col1_data, col2_data].into_iter()))
        .unwrap();
    let result = mem_rowset.flush().unwrap();
    let array1: &I32Array = (&result.arrays()[0]).try_into().unwrap();
    let array2: &F64Array = (&result.arrays()[1]).try_into().unwrap();
    assert_eq!(
        array1,
        &I32Array::from_iter(vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5]).into()
    );
    assert_eq!(
        array2,
        &F64Array::from_iter(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0
        ])
        .into()
    );
}