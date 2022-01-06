
use std::{
    fs, io,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, atomic::{Ordering, AtomicBool}},
    time::Duration,
};
use quinn::{Incoming, Endpoint, NewConnection};
use tokio::sync::mpsc::{self, Sender};
use anyhow::{Context, Result};
use futures_util::{StreamExt, stream::FuturesUnordered};
use crate::{
    NetMessage, MQMessage, BlockOn, ArcPointer,
    QMQ_SERVER, QMQ_QUIC_PRTOCOL, CERT_DIRECTORY,
};

//====================================================================================================================

type ClientMap = HashMap<usize, (NewConnection, AtomicBool)>;
type TopicMap = HashMap<String, HashSet<usize>>;

//====================================================================================================================

pub async fn start_server() {
    let (_, mut incoming) = open_listening().await.expect("failed to open listening");
    let (update_tx, mut update_rx) = mpsc::channel::<()>(100);
    let topic_map = ArcPointer::new(HashMap::<String, HashSet<usize>>::new());
    let client_map = ArcPointer::new(HashMap::<usize, (NewConnection, AtomicBool)>::new());

    let mut debug_update = tokio::time::interval(Duration::from_secs(2));

    println!("[SERVER] listening..");
    'outer: loop {
        let tick = debug_update.tick();
        let client = incoming.next();
        let rx = update_rx.recv();
        let mut bis_fut = FuturesUnordered::new();
        let mut unis_fut = FuturesUnordered::new();
        client_map
            .inner_mut()
            .iter_mut()
            .inspect(|(key, _)| println!("inspect key[{}]", key))
            .filter(|(_, (_, state))| !state.load(Ordering::SeqCst))
            .for_each(|(k, (conn, _state))| {
                bis_fut.push(async {
                    (*k, conn.bi_streams.next().await)
                });
                unis_fut.push(async {
                    (*k, conn.uni_streams.next().await)
                });
            });
        tokio::select! {
            Some(conn) = client => {
                println!("new connection ==========================================================================");
                tokio::spawn(handle_new_connection(
                    conn,
                    update_tx.clone(),
                    client_map.clone(),
                ));
            },
            Some((key, Some(bi))) = bis_fut.next() => {
                println!("bi stream ==========================================================================");
                tokio::spawn(handle_bi_stream(
                    key,
                    bi.map_err(anyhow::Error::new),
                    update_tx.clone(),
                    client_map.clone(),
                    topic_map.clone(),
                ));
            },
            Some((key, Some(uni))) = unis_fut.next() => {
                println!("uni stream ==========================================================================");
                tokio::spawn(handle_uni_stream(
                    key,
                    uni.map_err(anyhow::Error::new),
                    update_tx.clone(),
                    client_map.clone(),
                    topic_map.clone(),
                ));
            },
            Some(_) = rx => {
                println!("updated ==========================================================================");
                continue;
            },
            // _ = tick => {
            //     println!("tick ==========================================================================");
            //     continue;
            // },
            else => {
                break 'outer;
            }
        }
    }

    topic_map.clear();
    client_map.clear();
}

async fn open_listening() -> Result<(Endpoint, Incoming)> {

    let path = PathBuf::from(CERT_DIRECTORY);
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    let (cert, key) = match fs::read(&cert_path).and_then(|x| Ok((x, fs::read(&key_path)?))) {
        Ok(x) => x,
        Err(ref e) => if e.kind() == io::ErrorKind::NotFound {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
            let key = cert.serialize_private_key_der();
            let cert = cert.serialize_der()?;
            fs::create_dir_all(&path).context("failed to create certificate directory")?;
            fs::write(&cert_path, &cert).context("failed to write certificate")?;
            fs::write(&key_path, &key).context("failed to write private key")?;
            (cert, key)
        } else {
            anyhow::bail!("failed to read certificate {}", e);
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
        .ok_or(anyhow::anyhow!("Arc get_mut transport"))?
        .max_concurrent_uni_streams(0_u8.into())
        .max_idle_timeout(Some(Duration::from_millis(5_000).try_into()?))
        .keep_alive_interval(Some(Duration::from_millis(2_000)));
    server_config.use_retry(true);

    let socket: SocketAddr = QMQ_SERVER.parse()?;
    let (endpoint, incoming) = quinn::Endpoint::server(server_config, socket)?;

    Ok((endpoint, incoming))
}

async fn handle_new_connection(
    conn: quinn::Connecting,
    update_tx: Sender<()>,
    client_map: ArcPointer<ClientMap>,
) {
    match conn.await {
        Ok(new_conn) => {
            client_map
                .inner_mut()
                .entry(new_conn.connection.stable_id())
                .or_insert((new_conn, AtomicBool::new(false)));
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
    let client_map_clone = client_map.clone();
    if let Some((_, state)) = client_map_clone.inner().get(&key) {
        state.store(true, Ordering::SeqCst);

        match uni
            .and_then(|recv| {
                recv.read_to_end(usize::max_value()).block_on()
                    .map_err(anyhow::Error::new)
            })
            .and_then(|data| {
                bincode::deserialize::<MQMessage>(&data)
                    .map_err(anyhow::Error::new)
            })
            .and_then(|msg| async {
                println!("[SERVER] receive uni from client: [{:#?}]", msg);
                let client_map = client_map.inner();
                if let Some(set) = topic_map.inner().get(&msg.topic) {
                    set
                        .iter()
                        .map(|key| client_map.get(key).unwrap().0.connection.clone())
                        .for_each(|conn| {
                            let topic = msg.topic.clone();
                            let data = msg.data.clone();
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
                if let Some(conn_err) = e.downcast_ref::<quinn::ConnectionError>() {
                    match conn_err {
                        quinn::ConnectionError::ApplicationClosed(app_closed) => {
                            println!("[SERVER][Err] app closed: {}", app_closed);
                        },
                        quinn::ConnectionError::ConnectionClosed(conn_closed) => {
                            println!("[SERVER][Err] connection closed: {}", conn_closed);
                        },
                        _ => {},
                    }
                    client_map.inner_mut().remove(&key);
                    update_tx.send(()).await
                        .unwrap_or_else(|e| println!("[Err] send update state: {}", e));
                }
                if let Some(_read_err) = e.downcast_ref::<quinn::ReadToEndError>() {
                }
                println!("[SERVER][Err] handle uni stream: {}", e);
            },
        }

        state.store(false, Ordering::SeqCst);
    }
}

async fn handle_bi_stream(
    key: usize,
    bi: Result<(quinn::SendStream, quinn::RecvStream)>,
    update_tx: Sender<()>,
    client_map: ArcPointer<ClientMap>,
    topic_map: ArcPointer<TopicMap>,
) {
    let client_map_clone = client_map.clone();
    if let Some((_, state)) = client_map_clone.inner().get(&key) {
        state.store(true, Ordering::SeqCst);

        match bi
            .and_then(|(send, recv)| {
                recv.read_to_end(usize::max_value()).block_on()
                    .map_err(anyhow::Error::new)
                    .map(|data| (send, data))
            })
            .and_then(|(send, data)| {
                bincode::deserialize::<NetMessage>(&data)
                    .map_err(anyhow::Error::new)
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
                    .map_err(anyhow::Error::new)?;
                send.finish()
                    .await
                    // .map_err(|e| anyhow!("failed to shutdown stream: {}", e))?;
                    .map_err(anyhow::Error::new)?;

                Ok(())
            }.block_on())
        {
            Ok(_) => {},
            Err(e) => {
                if let Some(conn_err) = e.downcast_ref::<quinn::ConnectionError>() {
                    match conn_err {
                        quinn::ConnectionError::ApplicationClosed(app_closed) => {
                            println!("[SERVER][Err] app closed: {}", app_closed);
                        },
                        quinn::ConnectionError::ConnectionClosed(conn_closed) => {
                            println!("[SERVER][Err] connection closed: {}", conn_closed);
                        },
                        _ => {},
                    }
                    client_map.inner_mut().remove(&key);
                    update_tx.send(()).await
                        .unwrap_or_else(|e| println!("[Err] send update state: {}", e));
                }
                if let Some(_read_err) = e.downcast_ref::<quinn::ReadToEndError>() {
                }
                println!("[ERROR][Err] handle bi stream: {}", e);
            },
        }

        state.store(false, Ordering::SeqCst);
    }
}

//=================================================================================================

