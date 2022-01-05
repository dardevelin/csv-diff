use criterion::black_box;
use csv_diff::csv::Csv;
use csv_diff::csv_diff::*;
use std::io::Cursor;
use utils::csv_generator::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let csv_diff = CsvByteDiff::new()?;

    let (csv_gen_left, csv_gen_right) = (
        CsvGenerator::new(1_000_000, 9),
        CsvGenerator::new(1_000_000, 9),
    );

    let (csv_left, csv_right) = (csv_gen_left.generate(), csv_gen_right.generate());

    let res = black_box(
        csv_diff
            .diff(
                Csv::new(Cursor::new(csv_left.as_slice())),
                Csv::new(Cursor::new(csv_left.as_slice())),
            )
            .unwrap(),
    );
    Ok(())
}
