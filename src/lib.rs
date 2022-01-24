
use serde::{Serialize, Deserialize};

//====================================================================================================================

mod broker;
mod endpoint;
mod error;

pub use broker::start_broker;
pub use endpoint::QmqEndpoint;
pub use error::{Result, QmqError};
use std::sync::{Arc, atomic::{AtomicPtr, Ordering}};

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

struct ArcPointer<T>(Arc<AtomicPtr<T>>);
impl<T> ArcPointer<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(AtomicPtr::new(Box::into_raw(Box::new(inner)))))
    }
    pub fn new_raw(inner: *mut T) -> Self {
        Self(Arc::new(AtomicPtr::new(inner)))
    }
    pub fn inner(&self) -> &T {
        unsafe { &*self.0.load(Ordering::SeqCst) }
    }
    pub fn inner_mut(&self) -> &'static mut T {
        unsafe { &mut *self.0.load(Ordering::SeqCst) }
    }
    pub fn inner_raw(&self) -> *mut T {
        self.0.load(Ordering::SeqCst)
    }
    pub fn clear(self) {
        let _ = unsafe { Box::from_raw(self.0.load(Ordering::SeqCst)) };
        self.0.store(std::ptr::null_mut(), Ordering::Release);
    }
}
impl<T> Clone for ArcPointer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
