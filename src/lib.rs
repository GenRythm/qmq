
use serde::{Serialize, Deserialize};

//====================================================================================================================

mod broker;
mod endpoint;
mod error;

pub use broker::start_broker;
pub use endpoint::QmqEndpoint;
pub use error::{Result, QmqError};

//====================================================================================================================

const CERT_DIRECTORY: &str = "config/cert";
const QMQ_QUIC_PRTOCOL: &[&[u8]] = &[b"QUIC_MESSAGE_QUEUE"];
const MAX_UNI_CLIENTS: u8 = 100;

#[derive(Debug, Serialize, Deserialize)]
enum NetMessage {
    MessageQueue(MQMessage),
    Subscribe(String),
    Testing(String),
    OK,
    Ack,
}

#[derive(Debug, Serialize, Deserialize)]
struct MQMessage {
    topic: String,
    data: Vec<u8>,
}

trait BlockOn<T: std::future::Future> {
    fn block_on(self) -> T::Output;
}
impl<T: std::future::Future> BlockOn<T> for T {
    #[inline(always)]
    fn block_on(self) -> T::Output {
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(self)
        })
    }
}

