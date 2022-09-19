# 内存存储

有了表之后，我们就可以向其中导入数据了！数据在被持久化存储之前，会首先写入到内存中。
因此在本节中我们会先实现一个**内存存储系统（In-memory Storage）**。

对于分析型数据库（OLAP）而言，为了读取和计算的高效，它们大多使用**列式存储**，即同一列的数据在内存中被紧密地排列在一起。
这种数据结构就是我们所熟悉的**数组（Array）**。每一列的数据用一个数组表示，多个列的数据就组成了**数据块（DataChunk）**。
它们都是数据库中重要的基础类型。在我们的内存存储系统中，数据就是以 `DataChunk` 的形式存储在表内。并且在未来的执行引擎中，`DataChunk` 还是各个算子之间传递数据的基本单位。

在这个任务中，我们的主要目标就是以这两个类型为基础，构建一个简单的内存存储引擎。

<!-- toc -->

## 背景知识

### 事务型与分析型应用

目前数据库的主要应用场景可以分为两种：**联机事务处理**（On-line Transaction Processing，OLTP）和 **联机分析处理**（On-line Analytical Processing，OLAP）。
通常我们将其简称为 OLTP/OLAP，或事务型/分析型。

这两种场景的数据处理方式有很大区别。
事务型的工作负载侧重于对少量数据的查询与修改，比如查询指定 ID 的条目并更新其中的一些字段。每次操作要做的事情不多，但是频率会很高，并且对正确性、隔离性等有较强的要求。
分析型的工作负载则是大量数据上的复杂查询，以读取和计算为主。由于数据量非常巨大，一次查询的时间可能会很长。因此分析型任务一般更注重数据处理的吞吐量。

|id| name | age |
|--|------|-----|
| 1| Alice|  18 |
| 2| Bob  |  19 |
| 3| Eve  |  17 |

```sql
-- 事务型负载：更新学号为 1 的学生姓名
UPDATE student SET name = 'Tom' WHERE id = 1;

-- 分析型负载：查询学号前 10 位同学的平均年龄
SELECT avg(age) FROM student WHERE id <= 10;
```

在实际场景中，在线交易系统是一个很典型的事务型应用，而大数据分析系统则是典型的分析型应用。
由于它们的需求有很大差别，一个侧重写、一个侧重读，通常一个系统只能处理好一种类型的任务。
所以在实际使用中，它们二者一般是上下游的关系：数据首先在事务型系统中产生，然后再交给分析型系统进行处理。
但是这中间就会涉及数据的导入导出、格式转换等操作，带来额外的开销，延长整个数据处理链路的时间。
因此近年来，人们也在探索 **混合事务与分析处理**（Hybrid Transactional and Analytical Processing，HTAP），试图用一个系统同时解决这两个问题。

那么，事务型和分析型的区别到底会对数据库的设计产生哪些影响呢？

### 行式存储与列式存储

最主要的影响是数据的存储格式。

计算机中有一个很重要的原理是 **数据局部性（Data Locality）**：通常程序倾向于访问空间上连续的数据，并且处理器和存储设备也会利用这一特性对连续的数据访问进行优化。
因此利用好数据局部性往往能够获得更好的性能。

关系型数据库的一张表是一个二维表格，由此便产生了两种不同的数据存储格式：行式存储与列式存储。
在行式存储中，一行的数据被连续地存放在一起；相对地，在列式存储中，多个行中同一列的数据被连续地存放在一起。

TODO：图

回到数据库的访问模式上，我们容易发现：事务型负载倾向于一次读写一个完整的行，因此适合行式存储；而分析型负载往往只需要读取一部分列的数据，因此适合列式存储。
对于分析型数据库，使用列式存储不仅可以避免读取到不需要的数据，还可以让 CPU 用 SIMD 指令加速处理批量数据。
此外，同一列的相邻数据之间有时还具有相关性。比如对于一个有序的列，相邻的数据很有可能相同或者差别不大，这时就可以使用一些压缩编码格式来节省存储空间。
相关内容我们还会在 World 3 中具体介绍。

RisingLight 作为分析型数据库，自然会使用列式存储。接下来我们会讲一讲列式存储中的一点点细节。

### 数组与数据块

RisingLight 的列存引擎使用了计算机科学中最高级的数据结构——数组（Array）😜。

数组具有一系列非常好的性质：

- 所有元素在内存上连续排列，具有良好的空间局部性，缓存命中率高
- 在元素定长的情况下，每个元素的位置是固定的，支持根据下标随机访问

相对地，数组在需要动态修改时就没那么灵活了。不过对于分析型任务来说，我们只需要快速的顺序和随机读取，很少需要修改，因此数组就成为了最合适的数据结构。

对于定长元素的数组，几乎所有编程语言都有内置的支持，写起来很方便，就不再赘述了。

对于长度可变的元素，一般有两种实现方式：

1. 将所有元素存放在一个连续的 buffer 中，另一个下标数组存放每个元素起始位置
2. 每个元素位于不连续的动态内存中，数组存放指针

TODO：两种内存布局的表示图

例如对于可变长度的字符串数组，最常见的写法 `Vec<String>` 就是第二种实现。
但是这种方式的问题是动态内存分配的开销很大，每个元素都需要一次 malloc 和 free；并且每个元素在内存中的位置不一定连续，在顺序读取的场景下对缓存不友好。
相对地，第一种实现方式保证了所有元素都在内存中连续分布，比较适合分析型任务中一次构造、频繁读取的场景。

一个列的数据可以用一个数组表示，那么多个数组拼起来就可以表示一个表，或者对表数据处理的中间结果，我们称之为 **数据块（DataChunk）**。

TODO：DataChunk 和 Array 的结构图

DataChunk 是数据库执行引擎中对数据处理的基本单位。对于大批量数据的处理，执行引擎会将其切分成多个比较小的数据块。
这样一方面可以避免数组占用的连续内存过大，另一方面还有机会提高数据处理的并行度。

在当前任务中，我们的主要目标就是以 DataChunk 和 Array 类型为基础，构建 RisingLight 的内存存储引擎。

## 任务目标

实现数组 `Array`，支持根据下标随机访问和生成迭代器顺序访问。支持四种数据类型：布尔 `bool`、整数 `i32`、浮点数 `f64`、字符串 `&str`。

实现用来创建数组的 `ArrayBuilder`，并且推荐为数组实现 [`FromIterator`][] trait，以方便从一个迭代器 `collect()` 生成数组。

[`FromIterator`]: https://doc.rust-lang.org/std/iter/trait.FromIterator.html

最后，还需实现一个简单的内存存储系统，支持插入、删除表，并支持向表中插入数据和读取数据。

## 整体设计

一种可供参考的接口设计。：

```rust
// Array
pub trait Array: Sized + Send + Sync + 'static {
    type Builder: ArrayBuilder<Array = Self>;
    type Item: ToOwned + ?Sized;
    fn get(&self, idx: usize) -> Option<&Self::Item>;
    fn len(&self) -> usize;
    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait ArrayBuilder: Send + Sync + 'static {
    type Array: Array<Builder = Self>;
    fn with_capacity(capacity: usize) -> Self;
    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);
    fn append(&mut self, other: &Self::Array);
    fn finish(self) -> Self::Array;
}

pub trait Primitive:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
}

pub struct PrimitiveArray<T: Primitive> {...}
impl<T: Primitive> Array for PrimitiveArray<T> {...}

pub struct PrimitiveArrayBuilder<T: Primitive> {...}
impl<T: Primitive> ArrayBuilder for PrimitiveArrayBuilder<T> {...}

pub struct Utf8Array { ... }
impl Array for Utf8Array { ... }

pub struct Utf8ArrayBuilder {...}
impl ArrayBuilder for Utf8ArrayBuilder {...}

pub type BoolArray = PrimitiveArray<bool>;
pub type I32Array = PrimitiveArray<i32>;
pub type F64Array = PrimitiveArray<f64>;

pub enum ArrayImpl {
    Bool(BoolArray),
    Int32(I32Array),
    Float64(F64Array),
    Utf8(Utf8Array),
}

pub type BoolArrayBuilder = PrimitiveArrayBuilder<bool>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;

pub enum ArrayBuilderImpl {
    Bool(BoolArrayBuilder),
    Int32(I32ArrayBuilder),
    F64(F64ArrayBuilder),
    Utf8(Utf8ArrayBuilder),
}

// In-memory storage
pub struct InMemoryStorage {...}

impl InMemoryStorage {
    pub fn new() -> Self {...}
    pub fn add_table(&self, id: TableRefId) -> StorageResult<()> {...}
    pub fn get_table(&self, id: TableRefId) -> StorageResult<Arc<InMemoryTable>> {...}
}


pub struct InMemoryTable {...}

impl InMemoryTable {
    pub fn append(&self, chunk: DataChunk) -> StorageResult<()> {...}
    pub fn all_chunks(&self) -> StorageResult<Vec<DataChunkRef>> {...}
}
```

一种可供参考的代码结构：

```
.
├── array
│   ├── data_chunk.rs
│   ├── iter.rs
│   ├── mod.rs
│   ├── primitive_array.rs
│   └── utf8_array.rs
└── storage
    ├── memory
    │   ├── mod.rs
    │   └── table.rs
    └── mod.rs
```

除此之外，本节没有新增的 SQL 测试。
