# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- None

## 0.1.0-beta.0 (13. November, 2022)

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