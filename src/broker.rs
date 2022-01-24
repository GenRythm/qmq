
use std::{
    fs, io,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        // atomic::{Ordering, AtomicBool},
    },
    time::Duration,
};
use quinn::{Incoming, Endpoint, NewConnection};
use tokio::sync::mpsc::{self, Sender};
use futures_util::{StreamExt, stream::FuturesUnordered};
use crate::{
    NetMessage, MQMessage, BlockOn, ArcPointer, Result, QmqError,
    QMQ_QUIC_PRTOCOL, CERT_DIRECTORY, MAX_UNI_CLIENTS,
};

//====================================================================================================================

type ClientMap = HashMap<usize, NewConnection>;
type TopicMap = HashMap<String, HashSet<usize>>;

//====================================================================================================================

pub async fn start_broker(listen: SocketAddr) {
    let (_, mut incoming) = open_listening(listen).await.expect("failed to open listening");
    let (update_tx, mut update_rx) = mpsc::channel::<()>(100);
    let topic_map: ArcPointer<TopicMap> = ArcPointer::new(HashMap::new());
    let client_map: ArcPointer<ClientMap> = ArcPointer::new(HashMap::new());
    let mut bis_fut = FuturesUnordered::new();
    let mut unis_fut = FuturesUnordered::new();

    println!("[SERVER] listening..");
    'outer: loop {
        let client = incoming.next();
        let rx = update_rx.recv();
        bis_fut.clear();
        unis_fut.clear();
        client_map
            .inner_mut()
            .iter_mut()
            .for_each(|(k, conn)| {
                bis_fut.push(async {
                    (*k, conn.bi_streams.next().await)
                });
                unis_fut.push(async {
                    (*k, conn.uni_streams.next().await)
                });
            });
        tokio::select! {
            Some(conn) = client => {
                tokio::spawn(handle_new_connection(
                    conn,
                    update_tx.clone(),
                    client_map.clone(),
                ));
            },
            Some((key, Some(bi))) = bis_fut.next() => {
                tokio::spawn(handle_bi_stream(
                    key,
                    bi.map_err(QmqError::from),
                    update_tx.clone(),
                    client_map.clone(),
                    topic_map.clone(),
                ));
            },
            Some((key, Some(uni))) = unis_fut.next() => {
                tokio::spawn(handle_uni_stream(
                    key,
                    uni.map_err(QmqError::from),
                    update_tx.clone(),
                    client_map.clone(),
                    topic_map.clone(),
                ));
            },
            Some(_) = rx => {
                continue;
            },
            else => {
                break 'outer;
            }
        }
    }

    topic_map.clear();
    client_map.clear();
}

async fn open_listening(listen: SocketAddr) -> Result<(Endpoint, Incoming)> {

    let path = PathBuf::from(CERT_DIRECTORY);
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    let (cert, key) = match fs::read(&cert_path).and_then(|x| Ok((x, fs::read(&key_path)?))) {
        Ok(x) => x,
        Err(ref e) => if e.kind() == io::ErrorKind::NotFound {
            let cert = rcgen::generate_simple_self_signed(vec!["genrythm.cf".into(), "localhost".into()])?;
            let key = cert.serialize_private_key_der();
            let cert = cert.serialize_der()?;
            fs::create_dir_all(&path)?;
            fs::write(&cert_path, &cert)?;
            fs::write(&key_path, &key)?;
            (cert, key)
        } else {
            return Err(QmqError::CertificationError)
        }
    };
    let key = rustls::PrivateKey(key);
    let cert = rustls::Certificate(cert);

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    server_crypto.alpn_protocols = QMQ_QUIC_PRTOCOL.iter().map(|&x| x.into()).collect();

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));
    Arc::get_mut(&mut server_config.transport)
        // TODO other error
        .ok_or(QmqError::CommonError)?
        .max_concurrent_uni_streams(MAX_UNI_CLIENTS.into())
        .max_idle_timeout(Some(Duration::from_millis(5_000).try_into().map_err(|_| QmqError::VarIntBoundExceed)?))
        .keep_alive_interval(Some(Duration::from_millis(2_000)));
    server_config.use_retry(true);

    let (endpoint, incoming) = quinn::Endpoint::server(server_config, listen)?;

    Ok((endpoint, incoming))
}

async fn handle_new_connection(
    conn: quinn::Connecting,
    update_tx: Sender<()>,
    client_map: ArcPointer<ClientMap>,
) {
    match conn.await {
        Ok(new_conn) => {
            println!("[SERVER] new connection from [{}]", new_conn.connection.remote_address());
            client_map
                .inner_mut()
                .entry(new_conn.connection.stable_id())
                .or_insert(new_conn);
            update_tx.send(()).await
                .unwrap_or_else(|e| println!("[Err] send update state: {}", e));
        },
        Err(e) => println!("[Err] Await new connection: {}", e),
    }
}

async fn handle_uni_stream(
    key: usize,
    uni: Result<quinn::RecvStream>,
    update_tx: Sender<()>,
    client_map: ArcPointer<ClientMap>,
    topic_map: ArcPointer<TopicMap>,
) {
    match uni
        .and_then(|recv| {
            recv.read_to_end(usize::max_value())
                .block_on()
                .map_err(QmqError::from)
        })
        .and_then(|recv_data| {
            let decode = bincode::deserialize::<NetMessage>(&recv_data)
                .map_err(QmqError::from)?;
            if let NetMessage::MessageQueue(MQMessage { topic, .. }) = decode {
                Ok((topic, recv_data))
            } else {
                Err(QmqError::CommonError)
            }
        })
        .and_then(|(topic, data)| async {
            let client_map = client_map.inner();
            if let Some(set) = topic_map.inner().get(&topic) {
                set
                    .iter()
                    .filter_map(|key| {
                        client_map.get(key).map(|new_conn| new_conn.connection.clone())
                    })
                    .for_each(|conn| {
                        let data = data.clone();
                        let topic = topic.clone();
                        tokio::spawn(async move {
                            if let Ok(mut send) = conn.open_uni().await {
                                send.write_all(&data).await
                                    .unwrap_or_else(|e| {
                                        println!("[Err] client[{}] write topic[{}]: {}", key, topic, e)
                                    });
                                send.finish().await
                                    .unwrap_or_else(|e| {
                                        println!("[Err] client[{}] finish stream topic[{}]: {}", key, topic, e)
                                    });
                            }
                        });
                    });
            }

            Ok(())
        }.block_on())
    {
        Ok(_) => {},
        Err(e) => {
            println!("[SERVER][Err] handle uni stream: {:#?}", e);
            #[allow(clippy::single_match)]
            match e {
                QmqError::ConnectionError(_conn_err) => {
                    client_map.inner_mut().remove(&key);
                    update_tx.send(()).await
                        .unwrap_or_else(|e| println!("[Err] send update state: {}", e));
                },
                _ => {},
            }
        },
    }
}

async fn handle_bi_stream(
    key: usize,
    bi: Result<(quinn::SendStream, quinn::RecvStream)>,
    update_tx: Sender<()>,
    client_map: ArcPointer<ClientMap>,
    topic_map: ArcPointer<TopicMap>,
) {
    match bi
        .and_then(|(send, recv)| {
            recv.read_to_end(usize::max_value()).block_on()
                .map_err(QmqError::from)
                .map(|data| (send, data))
        })
        .and_then(|(send, data)| {
            bincode::deserialize::<NetMessage>(&data)
                .map_err(QmqError::from)
                .map(|msg| (send, msg))
        })
        .and_then(|(mut send, msg)| async {
            println!("[SERVER] receive from client: [{:#?}]", msg);
            match msg {
                NetMessage::Subscribe(topic) => {
                    topic_map
                        .inner_mut()
                        .entry(topic)
                        .and_modify(|set| { set.insert(key); })
                        .or_insert_with(|| {
                            let mut set = HashSet::new();
                            set.insert(key);
                            set
                        })
                        ;
                },
                NetMessage::Testing(_test) => {
                },
                _ => {},
            };
            let resp = bincode::serialize::<NetMessage>(&NetMessage::OK).unwrap();
            send.write_all(&resp)
                .await
                // .map_err(|e| anyhow!("failed to send response: {}", e))?;
                .map_err(QmqError::from)?;
            send.finish()
                .await
                // .map_err(|e| anyhow!("failed to shutdown stream: {}", e))?;
                .map_err(QmqError::from)?;

            Ok(())
        }.block_on())
    {
        Ok(_) => {},
        Err(e) => {
            println!("[ERROR][Err] handle bi stream: {:#?}", e);
            #[allow(clippy::single_match)]
            match e {
                QmqError::ConnectionError(_conn_err) => {
                    client_map.inner_mut().remove(&key);
                    update_tx.send(()).await
                        .unwrap_or_else(|e| println!("[Err] send update state: {}", e));
                },
                _ => {},
            }
        },
    }
}

//=================================================================================================

