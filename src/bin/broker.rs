
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .enable_all()
        .build()
        .unwrap()
        .block_on(qmq::start_broker("[::1]:9010".parse().expect("failed to parse socker address")));
}

