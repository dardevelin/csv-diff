#![cfg(feature = "rayon-threads")]

use ::csv_diff::csv::{Csv, IoArcAsRef};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use csv_diff::csv_diff;
use std::{fmt::Display, io::Cursor};
use utils::csv_generator::CsvGenerator;

fn criterion_benchmark(c: &mut Criterion) {
    let csv_byte_diff_local = csv_diff::CsvByteDiffLocal::new().expect("must be constructable");
    let mut csv_byte_diff = csv_diff::CsvByteDiff::new().expect("must be constructable");

    let mut bench_group_csv_diff_equal_csv = c.benchmark_group("csv_diff_equal_csv");

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

        bench_group_csv_diff_equal_csv.measurement_time(std::time::Duration::from_secs(
            if csv_gen_left.rows() == 100_000 || csv_gen_right.rows() == 100_000 {
                60
            } else if csv_gen_left.rows() == 1_000_000 || csv_gen_right.rows() == 1_000_000 {
                150
            } else {
                20
            },
        ));
        bench_group_csv_diff_equal_csv
            .throughput(Throughput::Bytes((csv_left.len() + csv_right.len()) as u64));
        bench_group_csv_diff_equal_csv.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "csv_byte_diff_local/{} <> {}",
                csv_gen_left, csv_gen_right
            )),
            &(&csv_left, &csv_right),
            |b, (csv_left, _csv_right)| {
                b.iter(|| {
                    csv_byte_diff_local
                        .diff(
                            Csv::with_reader_seek(Cursor::new(csv_left.as_slice())),
                            Csv::with_reader_seek(Cursor::new(csv_left.as_slice())),
                        )
                        .unwrap();
                });
            },
        );

        bench_group_csv_diff_equal_csv.bench_function(
            BenchmarkId::from_parameter(format!(
                "csv_byte_diff/{} <> {}",
                csv_gen_left, csv_gen_right
            )),
            |b| {
                b.iter_batched(
                    || {
                        (
                            String::from_utf8(csv_left.clone()).expect("is utf8"),
                            String::from_utf8(csv_left.clone()).expect("is utf8"),
                        )
                    },
                    |(csv_left_1, csv_left_2)| {
                        csv_byte_diff
                            .diff(
                                Csv::with_reader_seek(Cursor::new(IoArcAsRef(io_arc::IoArc::new(
                                    csv_left_1,
                                )))),
                                Csv::with_reader_seek(Cursor::new(IoArcAsRef(io_arc::IoArc::new(
                                    csv_left_2,
                                )))),
                            )
                            .for_each(drop);
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
