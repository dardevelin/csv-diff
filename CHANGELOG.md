# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- None

## 0.1.0-beta.4 (26. February, 2023)

### Changed
- Change MSRV to 1.63. This is in preparation to using scoped threads from std instead of from `crossbeam-utils` ([!24](https://gitlab.com/janriemer/csv-diff/-/merge_requests/24)).
- Bump dep `csv` to 1.2 - note that it is not locked behind `~` anymore, because this has caused build problems
in `qsv` (`qsv` requires `csv` v1.2)([!24](https://gitlab.com/janriemer/csv-diff/-/merge_requests/24)).

## 0.1.0-beta.3 (23. February, 2023)

### Changed
- __Breaking:__ Remove `TryFrom<DiffByteRecordsIterator>` impl on `DiffByteRecords` and instead
provide a method `try_to_diff_byte_records`: The reason is, that this conversion effectively `collect`s
the iterator and is therefore not a cheap conversion (as the previous `try_from` implied) ([2296131](https://gitlab.com/janriemer/csv-diff/-/commit/3a57681697ccb86b804c11af316318c71d434fae)).

## 0.1.0-beta.2 (19. February, 2023)

### Added
- Add new method `sort_by_columns` on `DiffByteRecords` ([!21](https://gitlab.com/janriemer/csv-diff/-/merge_requests/21)).
This method allows to sort the resulting diff by columns. _Thank you, [@jqnatividad](https://github.com/jqnatividad), for the idea!_

#### Examples
```rust
let csv_diff = csv_diff::csv_diff::CsvByteDiffBuilder::new().build()?;
let csv_left = "\
                header1,header2,header3\n\
                a,_,c\n\
                c,_,_";
let csv_right = "\
                header1,header2,header3\n\
                a,_,d\n\
                b,_,_";
let diff_res = csv_diff.diff(
    Csv::with_reader_seek(Cursor::new(csv_left.as_bytes())),
    Csv::with_reader_seek(Cursor::new(csv_right.as_bytes())),
);

let mut diff_res: DiffByteRecords = diff_res.try_into()?;

diff_res.sort_by_columns(vec![0])?;

let diff_rows_actual = diff_res.as_slice();

let diff_rows_expected = vec![
    DiffByteRecord::Modify {
        delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "_", "c"]), 2),
        add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "_", "d"]), 2),
        field_indices: vec![2],
    },
    DiffByteRecord::Add(ByteRecordLineInfo::new(
        csv::ByteRecord::from(vec!["b", "_", "_"]),
        3,
    )),
    DiffByteRecord::Delete(ByteRecordLineInfo::new(
        csv::ByteRecord::from(vec!["c", "_", "_"]),
        3,
    )),
];

assert_eq!(diff_rows_actual, diff_rows_expected.as_slice());
```

You can also sort by multiple columns (it will sort by the next column, if two records of the current column
have compared as equal):
```rust
let csv_diff = csv_diff::csv_diff::CsvByteDiffBuilder::new().build()?;
let csv_left = "\
                header1,header2,header3\n\
                a,10,c\n\
                c,1,x";
let csv_right = "\
                header1,header2,header3\n\
                a,10,d\n\
                b,1,xx";
let diff_res = csv_diff.diff(
    Csv::with_reader_seek(Cursor::new(csv_left.as_bytes())),
    Csv::with_reader_seek(Cursor::new(csv_right.as_bytes())),
);

let mut diff_res: DiffByteRecords = diff_res.try_into()?;

diff_res.sort_by_columns(vec![1, 2])?;

let diff_rows_actual = diff_res.as_slice();

let diff_rows_expected = vec![
    DiffByteRecord::Delete(ByteRecordLineInfo::new(
        csv::ByteRecord::from(vec!["c", "1", "x"]),
        3,
    )),
    DiffByteRecord::Add(ByteRecordLineInfo::new(
        csv::ByteRecord::from(vec!["b", "1", "xx"]), // "xx" is larger than "x" (as in ASCII), therefore it is sorted this way
        3,
    )),
    DiffByteRecord::Modify {
        delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "10", "c"]), 2), // "10" is larger than "1" (as in ASCII), so the order has been determined without looking at the second index
        add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "10", "d"]), 2),
        field_indices: vec![2],
    },
];

assert_eq!(diff_rows_actual, diff_rows_expected.as_slice());
```

- Add new enum `ColumnIdx`, which can be used in new method `sort_by_column` ([!21](https://gitlab.com/janriemer/csv-diff/-/merge_requests/21)):

```rust
// the following...
diff_byte_records.sort_by_columns(vec![1])?;

// ...can also be written as
diff_byte_records.sort_by_columns(vec![ColumnIdx::IdxForBoth(1)])?;
```
Currently, this enum only has one variant (`IdxForBoth(usize)`), but we plan to extend it, so that you can later sort by header name, too (e.g. via `ColumnIdx::HeaderForBoth(AsRef<[u8]>`)).

- Add new error enum `ColumnIdxError`, which is returned, when method `sort_by_column` tries to access a column that exceeds the number of columns in the CSV.

### Changed
- Lock dep `csv` to version ~1.1   
Version 1.2 is not compatible with MSRV of 1.56
- Add crate `xxhash-rust` to `Credits` in README (sorry for not mentioning it before - without it, this crate would not have been possible)

### Fixed
- None

## 0.1.0-beta.1 (7. January, 2023)

### Added
- Add a `From` impl on `Csv` for `csv::Reader` ([!19](https://gitlab.com/janriemer/csv-diff/-/merge_requests/19))

### Changed
- __breaking__: Allow `Csv<R>` to be directly constructed from `csv::Reader<R>` ([!19](https://gitlab.com/janriemer/csv-diff/-/merge_requests/19)).   
For example we can do the following:

```rust
let csv_left = "\
                header1,header2,header3";
let csv_right = "";

let diff_res_actual = CsvByteDiffLocal::new()?
    .diff(
        csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader_seek(csv_left.as_bytes())
            .into(),
        csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader_seek(csv_right.as_bytes())
            .into(),
    )
    .unwrap();
```

WARNING: Although this API seems very elegant at first, it has a _major_
shortcoming: we currently can't handle the case, where the `csv::Reader`
has been constructed with the setting `flexible` (or at least we
haven't tested it yet; see also the [flexible option on `csv::ReaderBuilder`](https://docs.rs/csv/latest/csv/struct.ReaderBuilder.html#method.flexible).
- __breaking__: Remove `Clone` derive on `Csv<R>` ([!19](https://gitlab.com/janriemer/csv-diff/-/merge_requests/19))

### Fixed
- None

## 0.1.0-beta.0 (4. December, 2022)

### Added
- Add new struct `CsvByteDiff` (the existing struct has been renamed `CsvByteDiff` -> `CsvByteDiffLocal`).
This new struct allows to diff CSVs in a "streaming fashion", which means calling `diff` on it will return a
`DiffByteRecordIterator`. Internally, processing the CSV records will fill up the internal channel with up to
10,000 elements. If `next()` is not called up until that point, no further CSV records are processed,
until `next()` is called again on the iterator. Please keep in mind, that results of the iterator
__are not sorted by line__.
They are only sorted in the sense that first, all variants of type `DiffByteRecord::Modify(..)` are emitted
and only after that the variants `DiffByteRecord::Add(..)` and `DiffByteRecord::Delete(..)` will follow
([!12](https://gitlab.com/janriemer/csv-diff/-/merge_requests/12)).
- impl `Clone` for `DiffByteRecords` and it's children (`DiffByteRecord`, `ByteRecordLineInfo`) ([b289fd1b](https://gitlab.com/janriemer/csv-diff/-/commit/b289fd1b37ff8f165c1b2f462665b89b2b5b12ec))
- provide a way to construct a `Csv`, where it's `Reader` doesn't require `Seek` ([fc51011d](https://gitlab.com/janriemer/csv-diff/-/commit/fc51011d44a6d6a8859d4fc3870be39f1754c1c2))
- Add new traits `CsvHashTaskSpawnerLocal` and `CsvHashTaskSpawnerLocalBuilder` (they replace the existing traits `CsvHashTaskSpawner` and `CsvHashTaskSpawnerBuilder` respectively)
- Add new struct `CsvHashTaskSpawnerStdThreads`, so that threads from std can be used with `CsvByteDiff`
- Add new struct `CsvHashTaskSpawnerBuilderStdThreads`
- Add new struct `CsvHashTaskSenderWithRecycleReceiver`
- Add new struct `CsvHashTaskSpawnerLocalBuilderRayon` (replaces existing struct `CsvHashTaskSpawnerBuilderRayon`)

### Changed
- __breaking__: rename `CsvByteDiff` -> `CsvByteDiffLocal` ([!12](https://gitlab.com/janriemer/csv-diff/-/merge_requests/12), [68176ecd](https://gitlab.com/janriemer/csv-diff/-/commit/68176ecde4c898a368be36dd9a8a75c2c6b14a89)).
The previous name is now used for a new struct that can provide the diff result in an iterator.
- __breaking__: rename `CsvByteDiffBuilder` -> `CsvByteDiffLocalBuilder`. The previous name is now used for a struct that
can build `CsvByteDiff`s.
- __breaking__: rename `CsvHashTaskSpawner` -> `CsvHashTaskSpawnerLocal`
- __breaking__: rename `CsvHashTaskSpawnerBuilder` -> `CsvHashTaskSpawnerBuilderLocal`
- __breaking__: rename `CsvHashTaskSpawnerBuilderRayon` -> `CsvHashTaskSpawnerLocalBuilderRayon`
- __breaking__: rename `CsvHashTaskSenders` -> `CsvHashTaskLineSenders`
- __breaking__: Enable passing CSV data directly to `Csv` ([beaec6f9](https://gitlab.com/janriemer/csv-diff/-/commit/beaec6f94f0538e0a904af152085cc7f0301cec4), [fc51011d](https://gitlab.com/janriemer/csv-diff/-/commit/fc51011d44a6d6a8859d4fc3870be39f1754c1c2))  
Before this change, one needed to wrap their CSV data into a `Cursor<T>`, before passing it to  `Csv::new` or `CsvBuilder::new`,
when their data wasn't already `Seek`able (e.g. bytes or a String):

Before:
```rust
let mut diff_byte_records = csv_byte_diff.diff(
    // we need to wrap our bytes in a cursor, because it needs to be `Seek`able
    Csv::new(Cursor::new(csv_data_left.as_bytes())),
    Csv::new(Cursor::new(csv_data_right.as_bytes())),
)?;
```
Now, we have removed the `new` method on `Csv` and `CsvBuilder` and instead provide the
associated methods `with_reader` and `with_reader_seek`.  
Now:
```rust
let mut diff_byte_records = csv_byte_diff.diff(
    // no need for `Cursor::new` anymore
    Csv::with_reader_seek(csv_data_left.as_bytes()),
    Csv::with_reader_seek(csv_data_right.as_bytes()),
)?;
```
This is possible, because `Csv::with_reader_seek` (and `CsvBuilder::with_reader_seek`) now accept
a __new trait__ `CsvReadSeek`, which can transform a non-`Seek`able read
into a `Seek`able read as long as the underlying data structure implements
AsRef<[u8]> (+ Send).
- Use `AHashMap` instead of `std::HashMap`. This improved performance of about 10% ([48e91bc](https://gitlab.com/janriemer/csv-diff/-/commit/48e91bc68b0546e1aa193eefc69d3c4184d1c55a)).
- Update docs: performance for diffing large amount of rows with `CsvByteDiffLocal` has decreased from ~500ms to ~600ms
- Bump MSRV to 1.56 ([25f512c7](https://gitlab.com/janriemer/csv-diff/-/commit/25f512c72ceb702f77d35f7588bf71f5c39b96d3))

### Fixed
- Use `xxhash128` with 128 bits instead of `ahash` with 64 bits ([!10](https://gitlab.com/janriemer/csv-diff/-/merge_requests/10)).
Using [ahash for equality comparison is very dangerous](https://users.rust-lang.org/t/using-ahash-non-cryptographic-hash-for-equality-comparison-considered-harmful/71701),
because the probability of collision is ~50% when hashing 2³² values. With `xxhash128`
we are producing hashes of 128 bits, which results in a far lower collision probability (~50% when hashing 2⁶⁴ values).
Please keep in mind that those values only hold true for perfect hash functions. __xxhash is not a perfect hash
function nor a cryptographic one__, but it scores _very well_ on the official [SMHasher](https://github.com/aappleby/smhasher) hashing [benchmark tests](https://github.com/rurban/smhasher).
There are no known collision in the hash values of xxhash with 128 bits.
- __breaking__: Make sorting of `DiffByteRecord`s more precise ([!13](https://gitlab.com/janriemer/csv-diff/-/merge_requests/13)).
The problem was that when two `DiffByteRecord`s had the same line number, sorting was undeterministic.
Now when two line numbers are equal, it additionally looks at the variant of the `DiffByteRecord`, where the following rules hold:
    - `DiffByteRecord::Delete(..)` < `DiffByteRecord::Modify(..)` < `DiffByteRecord::Add(..)`
- Remove internal buffer, when sending CSV hashes into channel. Previously, CSV hashes
have been buffered in a `smallvec` and then send into the channel. This led to optimal
performance, when diffing a lot of rows (~1 million), but it could lead to a stack overflow.
For diffing many rows, performance decreased of about 15-20% due to this change.