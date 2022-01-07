
use std::{time::Duration, sync::{atomic::{AtomicU64, Ordering}, Arc}};

const QMQ_SERVER: &str = "127.0.0.1:4433";

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .enable_all()
        .build()
        .unwrap()
        .block_on(event_loop());
}

async fn event_loop() {
    let endpoint = qmq::ArcPointer::new(qmq::QmqEndpoint::new(QMQ_SERVER).unwrap());

    let debug_count = Arc::new(AtomicU64::new(0));
    let mut debug_update = tokio::time::interval(Duration::from_secs(3));
    loop {
        let tick = debug_update.tick();
        let exit = tokio::signal::ctrl_c();
        tokio::select! {
            _ = tick => {
                tokio::spawn(simulate_send_uni(
                    debug_count.clone(),
                    endpoint.clone(),
                ));
            },
            _ = exit => { break; }
        }
    }

    endpoint.clear();
}

async fn simulate_send_uni(
    count: Arc<AtomicU64>,
    endpoint: qmq::ArcPointer<qmq::QmqEndpoint>,
) {
    println!("[PUB] send [{}]", count.load(Ordering::SeqCst));
    let topic = "topic0".to_string();
    endpoint
        .inner_mut()
        .send_to_topic(&topic, &count.load(Ordering::SeqCst).to_be_bytes())
        .await
        .unwrap_or_else(|e| println!("[ERROR] send to topic[{}]: {:#?}", topic, e));

    count.fetch_add(1, Ordering::SeqCst);
}
