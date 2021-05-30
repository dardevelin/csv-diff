#![cfg(feature = "rayon-threads")]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use csv_diff::csv_diff;
use std::{fmt::Display, io::Cursor};
use utils::csv_generator::CsvGenerator;

fn criterion_benchmark(c: &mut Criterion) {
    let csv_diff = csv_diff::CsvDiff::new();

    let mut bench_group = c.benchmark_group("csv_diff");

    for (csv_gen_left, csv_gen_right) in vec![
        (CsvGenerator::new(4, 3), CsvGenerator::new(4, 3)),
        (CsvGenerator::new(4, 10), CsvGenerator::new(4, 10)),
        (CsvGenerator::new(4, 100), CsvGenerator::new(4, 100)),
        (CsvGenerator::new(4, 1000), CsvGenerator::new(4, 1000)),
        (CsvGenerator::new(10, 9), CsvGenerator::new(10, 9)),
        (CsvGenerator::new(100, 9), CsvGenerator::new(100, 9)),
        (CsvGenerator::new(1000, 9), CsvGenerator::new(1000, 9)),
        (CsvGenerator::new(10_000, 9), CsvGenerator::new(10_000, 9)),
        (CsvGenerator::new(100_000, 9), CsvGenerator::new(100_000, 9)),
        (
            CsvGenerator::new(1_000_000, 9),
            CsvGenerator::new(1_000_000, 9),
        ),
    ] {
        let (csv_left, csv_right) = (csv_gen_left.generate(), csv_gen_right.generate());

        bench_group.measurement_time(std::time::Duration::from_secs(
            if csv_gen_left.rows() == 100_000 || csv_gen_right.rows() == 100_000 {
                30
            } else if csv_gen_left.rows() == 1_000_000 || csv_gen_right.rows() == 1_000_000 {
                100
            } else {
                10
            },
        ));
        bench_group.throughput(Throughput::Bytes((csv_left.len() + csv_right.len()) as u64));
        bench_group.bench_with_input(
            BenchmarkId::from_parameter(format!("{} <> {}", csv_gen_left, csv_gen_right)),
            &(csv_left, csv_right),
            |b, (csv_left, csv_right)| {
                b.iter(|| {
                    csv_diff
                        .diff(
                            Cursor::new(csv_left.as_slice()),
                            Cursor::new(csv_left.as_slice()),
                        )
                        .unwrap();
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
