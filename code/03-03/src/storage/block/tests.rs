use bytes::Bytes;

use super::{BlockBuilder, BlockIterator, PlainPrimitiveBlockBuilder, PlainPrimitiveBlockIterator};
use crate::array::{Array, ArrayBuilder, I32Array};

fn build_array<A: Array, F: FnOnce(&mut A::Builder)>(f: F) -> A {
    let mut builder = <A::Builder as ArrayBuilder>::with_capacity(0);
    f(&mut builder);
    builder.finish()
}

#[test]
fn test_i32_block_build_and_iterate() {
    let mut builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
    for i in 0..(32 - 1) {
        builder.append(Some(&i));
    }
    assert!(!builder.should_finish(&Some(&31)));
    builder.append(Some(&31));
    assert!(builder.should_finish(&Some(&32)));
    let block = Bytes::from(builder.finish());

    // When `expected_size` is `None`, iterator should return all information.
    let mut iter = PlainPrimitiveBlockIterator::<i32>::new(block.clone(), 32);
    let array = build_array::<I32Array, _>(|builder| {
        iter.next_batch(None, builder);
    });
    assert_eq!(array.len(), 32);
    for (idx, item) in array.iter().enumerate() {
        assert_eq!(*item.unwrap(), idx as i32);
    }

    // Test if `skip` functions currectly.
    let mut iter = PlainPrimitiveBlockIterator::<i32>::new(block.clone(), 32);
    iter.skip(10);
    let array = build_array::<I32Array, _>(|builder| {
        iter.next_batch(None, builder);
    });
    assert_eq!(array.len(), 22);
    for (idx, item) in array.iter().enumerate() {
        assert_eq!(*item.unwrap(), idx as i32 + 10);
    }

    // Test expected_size != None
    let mut iter = PlainPrimitiveBlockIterator::<i32>::new(block.clone(), 32);
    iter.skip(10);
    let array = build_array::<I32Array, _>(|builder| {
        iter.next_batch(Some(10), builder);
    });
    // Should return at least one element
    assert!(array.len() <= 10);
    assert!(array.len() > 0);
    for (idx, item) in array.iter().enumerate() {
        assert_eq!(*item.unwrap(), idx as i32 + 10);
    }

    // Test fetch multiple times
    let mut iter = PlainPrimitiveBlockIterator::<i32>::new(block.clone(), 32);
    let mut cnt = 0;
    for _ in 0..100 {
        let array = build_array::<I32Array, _>(|builder| {
            iter.next_batch(Some(10), builder);
        });
        cnt += array.len();
    }
    assert_eq!(cnt, 32);

    // Test skip across boundary
    let mut iter = PlainPrimitiveBlockIterator::<i32>::new(block, 32);
    let mut cnt = 0;
    iter.skip(2333);
    for _ in 0..100 {
        let array = build_array::<I32Array, _>(|builder| {
            iter.next_batch(Some(10), builder);
        });
        cnt += array.len();
    }
    assert_eq!(cnt, 0);
}
