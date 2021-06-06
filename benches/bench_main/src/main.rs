use criterion::black_box;
use csv_diff::csv_diff::*;
use csv_diff::diff_result::DiffRecords;
use std::io::Cursor;
use utils::csv_generator::*;

fn main() {
    let csv_diff = CsvDiff::new();

    let (csv_gen_left, csv_gen_right) = (
        CsvGenerator::new(1_000_000, 9),
        CsvGenerator::new(1_000_000, 9),
    );

    let (csv_left, csv_right) = (csv_gen_left.generate(), csv_gen_right.generate());

    let res = black_box(
        csv_diff
            .diff(
                Cursor::new(csv_left.as_slice()),
                Cursor::new(csv_left.as_slice()),
            )
            .unwrap(),
    );
}