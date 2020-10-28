use criterion::{black_box, criterion_group, criterion_main, Criterion};
use csv_diff::csv_diff;


fn criterion_benchmark(c: &mut Criterion) {
    let csv_left = "\
                    header1,header2,header3\n\
                    a,b,c\n\
                    d,e,f\n\
                    x,y,z";
    let csv_right = "\
                    header1,header2,header3\n\
                    a,b,c\n\
                    d,x,f\n\
                    x,y,z";
    
    let csv_diff = csv_diff::CsvDiff::new();
    c.bench_function("csv_diff - 4x3 - one modified", |b|
        b.iter(|| csv_diff.diff(black_box(csv_left.as_bytes()), black_box(csv_right.as_bytes())).unwrap()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);