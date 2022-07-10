csv-diff
==========================
### Find the difference between two CSVs - with ludicrous speed!🚀
---------

<!-- Tests status -->
  <a href="https://gitlab.com/janriemer/csv-diff">
    <img src="https://gitlab.com/janriemer/csv-diff/badges/main/pipeline.svg" />
  </a>
<!-- Crates version -->
  <a href="https://crates.io/crates/csv-diff">
    <img src="https://img.shields.io/crates/v/csv-diff.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/csv-diff">
    <img src="https://img.shields.io/crates/d/csv-diff.svg?style=flat-square"
      alt="Download" />
  </a>
  <!-- docs.rs docs -->
  <a href="https://docs.rs/csv-diff">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>

----------

## Documentation
https://docs.rs/csv-diff

### ⚠️Warning⚠️
This crate is still in it's infancy. There will be breaking changes (and dragons🐉) in the beginning.

## Highlights ✨
- fastest CSV-diffing library in the world🚀
    - compare two CSVs with 1,000,000 rows x 9 columns in __under 600ms__
- thread-pool agnostic 🧵🧶
    - use your existing thread-pool (e.g. [rayon][rayon]) or use threads directly (via [crossbeam][crossbeam-scope]); configurable via [Cargo features](#getting-started)
    - _caveat: thread-spawning must support [scoped threads][crossbeam-scope]_

[rayon]: https://docs.rs/rayon/1.5.0/rayon/
[crossbeam-scope]: https://docs.rs/crossbeam/0.8.0/crossbeam/thread/fn.scope.html

## Example
```rust
use std::io::Cursor;
use csv_diff::{csv_diff::CsvByteDiffLocal, csv::Csv};
use csv_diff::diff_row::{ByteRecordLineInfo, DiffByteRecord};
use std::collections::HashSet;
use std::iter::FromIterator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // some csv data with a header, where the first column is a unique id
    let csv_data_left = "id,name,kind\n\
                        1,lemon,fruit\n\
                        2,strawberry,fruit";
    let csv_data_right = "id,name,kind\n\
                        1,lemon,fruit\n\
                        2,strawberry,nut";

    let csv_byte_diff = CsvByteDiffLocal::new()?;

    let mut diff_byte_records = csv_byte_diff.diff(
        Csv::new(csv_data_left.as_bytes()),
        Csv::new(csv_data_right.as_bytes()),
    )?;

    diff_byte_records.sort_by_line();

    let diff_byte_rows = diff_byte_records.as_slice();

    assert_eq!(
        diff_byte_rows,
        &[DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["2", "strawberry", "fruit"]),
                3
            ),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["2", "strawberry", "nut"]), 3),
            field_indices: vec![2]
        }]
    );
    Ok(())
}
```

## Getting Started
In your Cargo.toml file add the following lines under `[dependencies]`:
```toml
csv-diff = "0.1.0-alpha"
```
This will use a rayon thread-pool, but you can opt-out of it and for example use threads without a thread-pool, by opting in into the `crossbeam-threads` feature (and opting-out of the default features):
```toml
csv-diff = { version = "0.1.0-alpha", default-features = false, features = ["crossbeam-threads"] }
```

## Use Case
This crate should be used on CSV data that has some sort of *primary key* for uniquely identifying a record.
It is __not__ a general line-by-line diffing crate.
You can imagine dumping a database table in CSV format from your *test* and *production* system and comparing it with each other to find differences.

## Caveats
Due to the fact that this crate is still in it's infancy, there are still some caveats, which we _might_ resolve in the near future:
- resulting CSV records/lines that have differences are provided as [raw bytes][ByteRecord]; you can use [`StringRecord::from_byte_record`](https://docs.rs/csv/1.1.6/csv/struct.StringRecord.html#method.from_byte_record) , provided by the [csv crate][csv], to try converting them into UTF-8 encoded records.
- CSVs must be [`Seek`][Seek]able
    - `Seek` is implemented for the most important types like:
        - [`File`][File]s
        - and when wrapped in a [`Cursor`][Cursor]
            - `String`s and `&str`
            - `[u8]`
- when using your own custom thread-pool, thread-spawning must support [scoped threads][crossbeam-scope]
- documentation must be improved

[csv]: https://docs.rs/csv/1.1.6/csv/
[Seek]: https://doc.rust-lang.org/std/io/trait.Seek.html
[ByteRecord]: https://docs.rs/csv/1.1.6/csv/struct.ByteRecord.html
[Cursor]: https://doc.rust-lang.org/std/io/struct.Cursor.html
[File]: https://doc.rust-lang.org/std/fs/struct.File.html

## Benchmarks
You can run benchmarks with the following command:
```shell
cargo bench
```

## Safety
This crate is implemented in __100% Safe Rust__, which is ensured by using `#![forbid(unsafe_code)]`.

## MSRV
The Minimum Supported Rust Version for this crate is __1.49__. An increase of MSRV will be indicated by a breaking change (according to SemVer).

## Credits
This crate is inspired by the CLI tool [csvdiff](https://github.com/aswinkarthik/csvdiff) by Aswin Karthik, which is written in Go. Definitely check it out. It is a great tool.

Additionally, this crate would not exist without the awesome Rust community and these fantastic crates 🦀:
- [ahash](https://docs.rs/ahash/0.7.6/ahash/)
- [crossbeam](https://docs.rs/crossbeam/0.8.1/crossbeam/)
- [crossbeam-channel](https://docs.rs/crossbeam-channel/0.5.1/crossbeam_channel/)
- [csv][csv]
- [mown](https://docs.rs/mown/0.2.1/mown/)
- [rayon][rayon]
- [smallvec](https://docs.rs/smallvec/1.7.0/smallvec/)
- [thiserror](https://docs.rs/thiserror/1.0.30/thiserror/)


<br>

-------

<br>

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
