
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

