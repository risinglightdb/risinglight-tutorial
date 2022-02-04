use bytes::Bytes;

use crate::array::I32Array;

use super::column_index::ColumnIndex;
use super::*;

#[tokio::test]
async fn test_i32_column_build_and_iterate() {
    let mut builder = I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_column_test());
    for i in 0..10 {
        let array = I32Array::from_iter(i * 1000..(i + 1) * 1000);
        builder.append(&array);
    }
    let (index, data) = builder.finish();
    let column_index = ColumnIndex::new(index);
    let data = Bytes::from(data);
    let column = Column::new(column_index, data);

    // Test iterator from start to end
    let mut iter = I32ColumnIterator::new(column, 0).await.unwrap();
    let mut cnt = 0;
    loop {
        let (_row_id, array) = iter.next_batch(Some(17)).await.unwrap().unwrap();
        for item in array.iter() {
            assert_eq!(*item.unwrap(), cnt);
            cnt += 1;
        }
        if cnt == 10 * 1000 {
            break;
        }
        if cnt > 10 * 1000 {
            panic!("more item than expected");
        }
    }
}
