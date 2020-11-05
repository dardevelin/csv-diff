use std::fmt::Display;
use criterion::{
    BenchmarkId,
    Throughput,
    black_box,
    criterion_group,
    criterion_main,
    Criterion
};
use csv_diff::csv_diff;

#[derive(Debug)]
pub struct CsvGenerator {
    rows: usize,
    columns: usize,
}

impl CsvGenerator {

    pub fn new(rows: usize, columns: usize) -> Self {
        Self {
            rows,
            columns,
        }
    }

    pub fn generate(&self) -> Vec<u8> {
        use fake::{
            Faker,
            Fake,
            faker::lorem::en::*
        };
        let mut headers = (1..=self.columns).map(|col| format!("header{}", col)).collect::<Vec<_>>().join(",");
        headers.push('\n');
        
        let rows = (0..self.rows())
            .map(|row_idx| {
                let mut row: Vec<String> = Words(self.columns..self.columns + 1).fake::<Vec<String>>();
                row[0] = row_idx.to_string();
                let mut row_string = row.join(",");
                row_string.push('\n');
                row_string.into_bytes()
            })
            .flatten()
            .collect();
        rows
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
}

impl Display for CsvGenerator { 
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}x{}", self.rows, self.columns)
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    
    let csv_diff = csv_diff::CsvDiff::new();


    let mut bench_group = c.benchmark_group("csv_diff");

    for (csv_gen_left, csv_gen_right) in vec![
        (CsvGenerator::new(4, 3), CsvGenerator::new(4, 3)),
        (CsvGenerator::new(4, 10), CsvGenerator::new(4, 10)),
        (CsvGenerator::new(4, 100), CsvGenerator::new(4, 100)),
        (CsvGenerator::new(4, 1000), CsvGenerator::new(4, 1000)),
        (CsvGenerator::new(10, 3), CsvGenerator::new(10, 3)),
        (CsvGenerator::new(100, 3), CsvGenerator::new(100, 3)),
        (CsvGenerator::new(1000, 3), CsvGenerator::new(1000, 3)),
        (CsvGenerator::new(10_000, 3), CsvGenerator::new(10_000, 3)),
        (CsvGenerator::new(100_000, 3), CsvGenerator::new(100_000, 3)),
        (CsvGenerator::new(1_000_000, 3), CsvGenerator::new(1_000_000, 3)),
        ] {
        let (csv_left, csv_right) = (csv_gen_left.generate(), csv_gen_right.generate());
        
        bench_group.measurement_time(std::time::Duration::from_secs(
            if csv_gen_left.rows() == 100_000 || csv_gen_right.rows() == 100_000 {
                15
            } else if csv_gen_left.rows() == 1_000_000 || csv_gen_right.rows() == 1_000_000 {
                70
            } else {
                5
            }));
        bench_group.throughput(Throughput::Bytes((csv_left.len() + csv_right.len()) as u64));
        bench_group.bench_with_input(BenchmarkId::from_parameter(format!("{} <> {}", csv_gen_left, csv_gen_right)), &(csv_left, csv_right), |b, (csv_left, csv_right)| {
            b.iter(|| csv_diff.diff(csv_left.as_slice(), csv_right.as_slice()).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);