use criterion::black_box;
use csv_diff::csv_diff::*;
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
            .diff(csv_left.as_slice(), csv_left.as_slice())
            .unwrap(),
    );
}
