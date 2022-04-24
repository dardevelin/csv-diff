use criterion::black_box;
use csv_diff::csv::{Csv, IoArcAsRef};
use csv_diff::csv_diff::*;
use std::io::Cursor;
use utils::csv_generator::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_byte_diff = CsvByteDiff::new()?;

    let (csv_gen_left, csv_gen_right) = (
        CsvGenerator::new(1_000_000, 9),
        CsvGenerator::new(1_000_000, 9),
    );

    let (csv_left, csv_right) = (csv_gen_left.generate(), csv_gen_right.generate());

    let csv_left_1: String = String::from_utf8(csv_left.clone()).expect("is utf8");
    let csv_left_2 = csv_left_1.clone();

    let res = black_box(
        csv_byte_diff
            .diff(
                Csv::with_reader_seek(Cursor::new(IoArcAsRef(io_arc::IoArc::new(csv_left_1)))),
                Csv::with_reader_seek(Cursor::new(IoArcAsRef(io_arc::IoArc::new(csv_left_2)))),
            )
            .for_each(drop),
    );
    Ok(())
}
