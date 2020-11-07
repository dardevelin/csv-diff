use utils::csv_generator::*;
use csv_diff::csv_diff::*;

fn main() {
    let test = CsvGenerator::new(4,3);
    dbg!(test);
    let csv_diff = CsvDiff::new();
    dbg!(csv_diff);

    let (csv_gen_left, csv_gen_right) = (CsvGenerator::new(100_000, 3), CsvGenerator::new(100_000, 3));

    let res = csv_diff.diff(csv_left.as_slice(), csv_right.as_slice()).unwrap();
}
