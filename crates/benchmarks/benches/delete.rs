use delta_benchmarks::{prepare_delete_input, run_direct_delete, run_sql_delete};
use divan::{AllocProfiler, Bencher};

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn bench_sql_delete(bencher: Bencher, stats: Option<&'static str>) {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    bencher
        .with_inputs(|| {
            runtime
                .block_on(prepare_delete_input(stats))
                .expect("prepare delete input")
        })
        .bench_local_values(|input| {
            runtime.block_on(async move {
                let batches = run_sql_delete(&input).await.expect("execute SQL DELETE");
                divan::black_box(batches);
            });
        });
}

fn bench_direct_delete(bencher: Bencher, stats: Option<&'static str>) {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    bencher
        .with_inputs(|| {
            runtime
                .block_on(prepare_delete_input(stats))
                .expect("prepare delete input")
        })
        .bench_local_values(|input| {
            runtime.block_on(async move {
                let result = run_direct_delete(input)
                    .await
                    .expect("execute direct DELETE");
                divan::black_box(result);
            });
        });
}

#[divan::bench]
fn sql_delete_with_stats(bencher: Bencher) {
    bench_sql_delete(bencher, Some(r#"{"numRecords":1024}"#));
}

#[divan::bench]
fn sql_delete_missing_stats_exact_count(bencher: Bencher) {
    bench_sql_delete(bencher, None);
}

#[divan::bench]
fn direct_delete_with_stats(bencher: Bencher) {
    bench_direct_delete(bencher, Some(r#"{"numRecords":1024}"#));
}

#[divan::bench]
fn direct_delete_missing_stats_unknown_count(bencher: Bencher) {
    bench_direct_delete(bencher, None);
}
