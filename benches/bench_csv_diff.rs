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
struct CsvGenerator {
    rows: u64,
    columns: usize,
}

impl CsvGenerator {

    pub fn new(rows: u64, columns: usize) -> Self {
        Self {
            rows,
            columns,
        }
    }

    pub fn generate(&self) -> Vec<u8> {
        let headers = (1..=self.columns).map(|col| format!("header{}", col)).collect::<Vec<_>>().join(",");
        


        //todo!()
        "\
        header1,header2,header3\n\
        a,b,c\n\
        d,e,f\n\
        x,y,z".into()
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

    for (csv_gen_left, csv_gen_right) in vec![(CsvGenerator::new(4, 3), CsvGenerator::new(4, 3))] {
        let (csv_right, csv_left) = (csv_gen_left.generate(), csv_gen_right.generate());
        bench_group.throughput(Throughput::Bytes((csv_left.len() + csv_right.len()) as u64));
        bench_group.bench_with_input(BenchmarkId::from_parameter(format!("{} <> {}", csv_gen_left, csv_gen_right)), &(csv_left, csv_right), |b, (csv_left, csv_right)| {
            b.iter(|| csv_diff.diff(csv_left.as_slice(), csv_right.as_slice()).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);