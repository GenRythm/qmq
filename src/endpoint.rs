
use std::{
    fs,
    // net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};
use futures_util::{AsyncWriteExt, StreamExt};
use quinn::{
    NewConnection,
    // RecvStream,
    Endpoint,
};
use crate::{
    CERT_DIRECTORY, QMQ_QUIC_PRTOCOL,
    NetMessage, BlockOn, Result, QmqError, MQMessage,
};

//====================================================================================================================

pub struct QmqEndpoint {
    conn: Option<NewConnection>,
}
impl QmqEndpoint {
    pub fn new(addr: &str) -> Result<Self> {
        let addr = addr.parse()?;
        let endpoint = open_client()?;
        let new_conn = endpoint
            .connect(addr, "localhost")?
            .block_on()?;
        Ok(Self {
            conn: Some(new_conn),
        })
    }
    pub async fn register_topic(&mut self, topic: &str) -> Result<()> {
        let (mut send, recv) = self.conn
            .as_mut()
            .ok_or(QmqError::ConnectionLost)?
            .connection
            .open_bi()
            .await?;

        let msg = NetMessage::Subscribe(topic.to_string());
        let encoded = bincode::serialize(&msg)?;
        send.write_all(&encoded)
            .await?;
        send.flush()
            .await?;
        send.finish()
            .await?;

        let read_data = recv
            .read_to_end(usize::max_value())
            .await?;

        match bincode::deserialize::<NetMessage>(&read_data)? {
            NetMessage::OK => Ok(()),
            _ => Err(QmqError::SubscribeError),
        }
    }
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        let recv = self.conn
            .as_mut()
            .ok_or(QmqError::ConnectionLost)?
            .uni_streams
            .next()
            .await
            .ok_or(QmqError::ConnectionLost)??
        ;
        let read_data = recv
            .read_to_end(usize::max_value())
            .await?;
        match bincode::deserialize::<NetMessage>(&read_data)? {
            NetMessage::MessageQueue(MQMessage {
                data,
                ..
            }) => {
                Ok(Some(data))
            },
            _ => Ok(None)
        }
    }
    pub async fn send_to_topic(&mut self, topic: &str, msg: &[u8]) -> Result<()> {
        let mut send = self.conn
            .as_mut()
            .ok_or(QmqError::ConnectionLost)?
            .connection
            .open_uni()
            .await?;

        let msg = NetMessage::MessageQueue(MQMessage {
            topic: topic.to_string(),
            data: msg.to_vec(),
        });
        let encoded = bincode::serialize(&msg)?;
        send.write_all(&encoded)
            .await?;
        send.flush()
            .await?;
        send.finish()
            .await?;

        Ok(())
    }
}

fn open_client() -> Result<Endpoint> {
    let mut roots = rustls::RootCertStore::empty();
    let path = PathBuf::from(CERT_DIRECTORY);
    let ca_path = path.join("cert.der");
    roots.add(&rustls::Certificate(fs::read(&ca_path)?)).map_err(|_| QmqError::CertificationError)?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    client_crypto.alpn_protocols = QMQ_QUIC_PRTOCOL.iter().map(|&x| x.into()).collect();
    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(client_crypto)));

    Ok(endpoint)
}

//====================================================================================================================

/*
pub async fn start_client() {
    let endpoint = open_client().expect("open client");
    let mut new_conn = endpoint
        .connect(QMQ_SERVER.parse::<SocketAddr>().unwrap(), "localhost")
        .unwrap()
        .await
        .map_err(|e| println!("[Err] connect: {}", e))
        .unwrap();

    send_subcribe(&new_conn, "topic0").await
        .map_err(|e| println!("[Err] send msg: {:#?}", e))
        .unwrap();

    // send_msg_uni(&new_conn, "Message uni 1").await
    //     .map_err(|e| println!("[Err] send msg: {}", e))
        // .unwrap();
    // tokio::time::sleep(Duration::from_millis(1000)).await;
    // send_msg(&new_conn, "Message2").await
    //     .map_err(|e| println!("[Err] send msg: {}", e))
    //     .unwrap();

    // new_conn.connection.close(quinn::VarInt::from_u32(0), "DONE".as_bytes());

    loop {
        let exit = tokio::signal::ctrl_c();
        let uni = new_conn.uni_streams.next();
        tokio::select! {
            Some(recv) = uni => {
                // tokio::spawn(recv_uni(recv.map_err(anyhow::Error::new)));
            },
            _ = exit => {
                break;
            },
        }
    }
}

async fn send_subcribe(new_conn: &NewConnection, topic: &str) -> Result<()> {
    let (mut send, recv) = new_conn.connection
        .open_bi()
        .await?;

    let msg = NetMessage::Subscribe(topic.to_string());
    let encoded = bincode::serialize(&msg)?;
    send.write_all(&encoded)
        .await?;
    send.flush()
        .await?;
    send.finish()
        .await?;

    let read_data = recv
        .read_to_end(usize::max_value())
        .await?;

    let resp = bincode::deserialize::<NetMessage>(&read_data)?;
    println!("[CLIENT] resp[{:#?}]", resp);

    Ok(())
}

async fn send_msg_uni(conn: &NewConnection, msg: &str) -> Result<()> {
    println!("begin");
    let mut send = conn.connection
        .open_uni()
        .await?;
    println!("after open uni");

    let msg = NetMessage::Testing(msg.to_owned());
    let encoded = bincode::serialize(&msg)?;
    send.write_all(&encoded)
        .await?;
    println!("after write_all uni");
    send.finish()
        .await?;
    println!("after finish uni");

    Ok(())
}

async fn recv_uni(recv: Result<quinn::RecvStream>) {
    match recv {
        Ok(recv) => {
            if let Ok(read_data) = recv
                .read_to_end(usize::max_value())
                .await
                .map_err(QmqError::from)
            {
                let msg = bincode::deserialize::<NetMessage>(&read_data).unwrap();
                println!("[CLIENT] resp[{:#?}]", msg);
            }
        },
        Err(e) => {
            println!("[CLIENT][Err] receive uni stream: {:#?}", e);
        },
    }
}
*/
