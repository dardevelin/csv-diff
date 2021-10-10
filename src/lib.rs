/*!
Find the difference between two CSVs - with ludicrous speed!🚀

`csv-diff` is a crate for finding the difference between two CSVs.
It is the fastest CSV-diffing library in the world!
Comparing two 1,000,000 rows x 9 columns CSVs takes __under 500ms__ (when using [raw bytes](csv_diff::CsvDiff::diff_bytes)).
It is *thread-pool-agnostic*, meaning you can provide your own existing thread-pool
or you can use the default [rayon thread-pool](https://docs.rs/rayon/1.5.0/rayon/struct.ThreadPool.html)
that is used in this crate.

# Use Case
This crate should be used on CSV data that has some sort of *primary key* for uniquely identifying a record.
It is __not__ a general line-by-line diffing crate.
You can imagine dumping a database table in CSV format from your *test* and *production* system and comparing it
with each other to find differences.

# Overview
The most important types you will use are:
1. [`CsvDiff`](csv_diff::CsvDiff) for comparing two CSVs.
2. [`DiffByteRecords`](diff_result::DiffByteRecords) for getting the actual differences between the compared CSVs.
*/

pub mod csv_diff;
mod csv_hash_comparer;
pub mod csv_hash_task_spawner;
pub mod csv_parse_result;
mod csv_parser_hasher;
pub mod diff_result;
pub mod diff_row;
mod thread_scope_strategy; // TODO: do we really need this?
