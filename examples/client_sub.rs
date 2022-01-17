
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .enable_all()
        .build()
        .unwrap()
        .block_on(event_loop());
}

async fn event_loop() {
    let endpoint = nonsense_util::ArcPointer::new(qmq::QmqEndpoint::new(qmq::QMQ_SERVER).unwrap());
    endpoint.inner_mut().register_topic("topic0").await
        .unwrap_or_else(|e| println!("[ERROR] register topic: {:#?}", e));

    loop {
        let exit = tokio::signal::ctrl_c();
        let msg_recv = endpoint.inner_mut().recv();
        tokio::select! {
            msg = msg_recv => { tokio::spawn(handle_msg(msg)); },
            _ = exit => { break; }
        }
    }

    endpoint.clear();
}

async fn handle_msg(msg: qmq::Result<Option<Vec<u8>>>) {
    match msg {
        Ok(Some(data)) => {
            let count = u64::from_be_bytes(data.try_into().unwrap());
            println!("[Endpoint] received count[{}]", count);
        },
        Ok(None) => {
            println!("[Endpoint] Ok None");
        }
        Err(e) => {
            println!("[Endpoint][ERROR] recv : {:#?}", e);
        },
    }
}

