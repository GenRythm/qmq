
use std::{
    fs,
    net::SocketAddr,
    path::PathBuf, sync::Arc, time::Duration,
};
use futures_util::AsyncWriteExt;
use quinn::{
    NewConnection,
    // RecvStream,
    Endpoint,
};
use anyhow::{Result, anyhow};
use crate::{CERT_DIRECTORY, QMQ_QUIC_PRTOCOL, QMQ_SERVER, NetMessage};

//====================================================================================================================

pub async fn start_client() {
    let endpoint = open_client().expect("open client");
    let new_conn = endpoint
        .connect(QMQ_SERVER.parse::<SocketAddr>().unwrap(), "localhost")
        .unwrap()
        .await
        .map_err(|e| println!("[Err] connect: {}", e))
        .unwrap();

    send_msg(&new_conn, "Message1").await
        .map_err(|e| println!("[Err] send msg: {}", e))
        .unwrap();
    // send_msg_uni(&new_conn, "Message uni 1").await
    //     .map_err(|e| println!("[Err] send msg: {}", e))
        // .unwrap();
    // tokio::time::sleep(Duration::from_millis(1000)).await;
    send_msg(&new_conn, "Message2").await
        .map_err(|e| println!("[Err] send msg: {}", e))
        .unwrap();

    new_conn.connection.close(quinn::VarInt::from_u32(0), "DONE".as_bytes());

    // loop {
    // }
}

async fn send_msg(new_conn: &NewConnection, msg: &str) -> Result<()> {
    let (mut send, recv) = new_conn.connection
        .open_bi()
        .await
        .map_err(|e| anyhow!("failed to open stream: {}", e))?;

    let msg = NetMessage::Testing(msg.to_owned());
    let encoded = bincode::serialize(&msg)?;
    send.write_all(&encoded)
        .await
        .map_err(|e| anyhow!("failed to send request: {}", e))?;
    send.flush()
        .await
        .map_err(|e| anyhow!("failed to send request: {}", e))?;
    send.finish()
        .await
        .map_err(|e| anyhow!("failed to shutdown stream: {}", e))?;

    let read_data = recv
        .read_to_end(usize::max_value())
        .await
        .map_err(|e| anyhow!("failed to read response: {}", e))?;

    let resp = bincode::deserialize::<NetMessage>(&read_data)?;
    println!("[CLIENT] resp[{:#?}]", resp);

    Ok(())
}

async fn send_msg_uni(new_conn: &NewConnection, msg: &str) -> Result<()> {
    println!("begin");
    let mut send = new_conn.connection
        .open_uni()
        .await
        .map_err(|e| anyhow!("failed to open stream: {}", e))?;
    println!("after open uni");

    let msg = NetMessage::Testing(msg.to_owned());
    let encoded = bincode::serialize(&msg)?;
    send.write_all(&encoded)
        .await
        .map_err(|e| anyhow!("failed to send request: {}", e))?;
    println!("after write_all uni");
    send.finish()
        .await
        .map_err(|e| anyhow!("failed to shutdown stream: {}", e))?;
    println!("after finish uni");

    Ok(())
}

fn open_client() -> Result<Endpoint> {
    let mut roots = rustls::RootCertStore::empty();
    let path = PathBuf::from(CERT_DIRECTORY);
    let ca_path = path.join("cert.der");
    roots.add(&rustls::Certificate(fs::read(&ca_path)?))?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    client_crypto.alpn_protocols = QMQ_QUIC_PRTOCOL.iter().map(|&x| x.into()).collect();
    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(client_crypto)));

    Ok(endpoint)
}

