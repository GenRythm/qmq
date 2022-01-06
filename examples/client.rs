
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .enable_all()
        .build()
        .unwrap()
        .block_on(qmq::start_client());
}

