use bincode;
use serde::{Serialize, Deserialize};

use std::net::SocketAddr;
use std::sync::Arc;

use proto;
use error::Error;


//------------------------------------------------
//
// message info
//
//------------------------------------------------

pub type Magic = u32;
pub const FRAME_MAGIC: Magic = 0x6D47_5709;

pub type RequestId = u64;

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    Handshake,
    Challenge,
    ChallengeResponse,
    HandshakeResponse,

    Rumor,
    RumorResponse,
    Advertise,
    AdvertiseResponse,

    // TODO: Application{Response} -- just a byte vec for application-specific messages
}
impl MessageType {
}

//------------------------------------------------
//
// message header and body
//
//------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub clock: proto::RequestId,
    pub magic: proto::Magic,
    pub data_len: u64,
    pub msg_type: MessageType,
}
impl Header {
    pub fn serialized_size() -> u64 {
        bincode::serialized_size(&Header{
            clock: 0,
            magic: proto::FRAME_MAGIC,
            data_len: 0,
            msg_type: MessageType::Handshake,
        })
    }

    pub fn from(clock: proto::RequestId, ty: MessageType, size: u64) -> Header {
        Header{
            clock: clock,
            magic: proto::FRAME_MAGIC,
            data_len: size,
            msg_type: ty,
        }
    }
}


#[derive(Debug, Clone)]
pub struct Message {
    pub hdr: Header,
    pub payload: Arc<Vec<u8>>,
}
impl Message {
    pub fn new<T>(clock: proto::RequestId, ty: MessageType, msg: T) -> Message 
        where T: Serialize
    {
        // TODO: not inifinite. sizeof<T> maybe?
        let ser = bincode::serialize(&msg, bincode::Infinite).expect("could not serialize payload");
        Message {
            hdr: Header::from(clock, ty, ser.len() as u64), // TODO: assert len < u64
            payload: Arc::new(ser),
        }
    }

    pub fn from_bytes(clock: proto::RequestId, ty: MessageType, msg: Vec<u8>) -> Message {
        // TODO: not inifinite. sizeof<T> maybe?
        Message {
            hdr: Header::from(clock, ty, msg.len() as u64), // TODO: assert len < u64
            payload: Arc::new(msg),
        }
    }

    pub fn from_heap(clock: proto::RequestId, ty: MessageType, msg: Arc<Vec<u8>>) -> Message {
        // TODO: not inifinite. sizeof<T> maybe?
        Message {
            hdr: Header::from(clock, ty, msg.len() as u64), // TODO: assert len < u64
            payload: msg,
        }
    }

    pub fn deframe<T>(&self) -> Result<T, Error>
        where T: FramedMessage,
              for<'de> T: Deserialize<'de>
    {
        if T::msg_type() != self.hdr.msg_type {
            return Err(Error::unexpected(
                vec!(T::msg_type()),
                self.hdr.msg_type.clone(),
                "failed to deframe".to_string()
            ));
        }

        // TODO: assert self.hdr.msg_type maps to T
        bincode::deserialize(&self.payload[..]).map_err(|e| e.into())
    }
}


//------------------------------------------------
//
// message body types
//
//------------------------------------------------

pub trait FramedMessage {
    fn to_msg(self, clock: proto::RequestId) -> Message;
    fn msg_type() -> MessageType;
    // TODO: max_msg_size() -> usize;    want to defeat data_len=u64::max() forged-packets
}


//
// handshake
//

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ClientType {
    /// This client is meant to only publish gossip.
    ///
    /// While not forbidden, signals that the receiver of the handshake
    /// should not consider this a peer in the cluster, but rather an
    /// edge node that simply injects work into the system. It then follows
    /// that the receiving node should not attempt to proxy rumors
    /// it has heard to this client.
    ///
    /// Futhermore, when a node/client asks for peers (handshake or otherwise)
    /// this node/client should be exempt.
    Publisher,

    /// This client is meant solely to listen.
    ///
    /// This type should probably be less common, but allows for aggregation
    /// of rumors. This is intended to be used on machines with more resources
    /// which can then work as a centralizing node for a smaller network.
    ///
    /// For instance, one listener per datacenter can schedule/proxy work
    /// without incurring the network/memory/disk resources needed to store
    /// or receive N rumors across all M machines.
    ///
    /// Listener has no real indication on whether the receiver of the handshake
    /// should expect gossip messages from the sender.
    Listener,

    /// This is a client that expects to send and receive rumors.
    ///
    /// When receiving a handshake of this type, it is ideal to open a mutual
    /// connection to this node if the receiver has open connections available.
    Full,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Handshake<I> {
    client_id: I,
    client_ty: ClientType,
    contact: SocketAddr,
}
impl<I> Handshake<I>
    where I: Serialize
{
    pub fn publisher(as_id: I, at: SocketAddr) -> Handshake<I> {
        Handshake{
            client_id: as_id,
            client_ty: ClientType::Publisher,
            contact: at,
        }
    }

    pub fn listener(as_id: I, at: SocketAddr) -> Handshake<I> {
        Handshake{
            client_id: as_id,
            client_ty: ClientType::Listener,
            contact: at,
        }
    }

    pub fn new(as_id: I, at: SocketAddr) -> Handshake<I> {
        Handshake{
            client_id: as_id,
            client_ty: ClientType::Full,
            contact: at,
        }
    }

    pub fn id(&self) -> &I {
        &self.client_id
    }

    pub fn ty(&self) -> &ClientType {
        &self.client_ty
    }

    pub fn route(&self) -> &SocketAddr {
        &self.contact
    }
}
impl<I> FramedMessage for Handshake<I>
    where I: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::Handshake, self)
    }

    fn msg_type() -> MessageType { MessageType::Handshake }
}


// TODO: allow saying "no" to a handshake due to crowding or other reasons
#[derive(Debug, Serialize, Deserialize)]
pub struct HandshakeResponse<I> {
    remote_id: I,
    peers: Vec<SocketAddr>,
}
impl<I> HandshakeResponse<I>
    where I: Serialize
{
    pub fn new(as_id: I, peers: Vec<SocketAddr>) -> HandshakeResponse<I> {
        HandshakeResponse{
            remote_id: as_id,
            peers: peers
        }
    }

    pub fn id(&self) -> &I {
        &self.remote_id
    }

    pub fn seeds(&self) -> &Vec<SocketAddr> {
        &self.peers
    }
}
impl<I> FramedMessage for HandshakeResponse<I>
    where I: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::HandshakeResponse, self)
    }

    fn msg_type() -> MessageType { MessageType::HandshakeResponse }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Challenge<C> {
    pub puzzle: C
}
impl<C> Challenge<C> {
    pub fn new(puzz: C) -> Challenge<C> {
        Challenge{
            puzzle: puzz,
        }
    }
}
impl<C> FramedMessage for Challenge<C>
    where C: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::Challenge, self)
    }

    fn msg_type() -> MessageType { MessageType::Challenge }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct ChallengeResponse<R> {
    pub answer: R
}
impl<R> ChallengeResponse<R> {
    pub fn new(answer: R) -> ChallengeResponse<R> {
        ChallengeResponse{
            answer: answer,
        }
    }
}
impl<R> FramedMessage for ChallengeResponse<R>
    where R: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::ChallengeResponse, self)
    }

    fn msg_type() -> MessageType { MessageType::ChallengeResponse }
}



//
// rumor
//

#[derive(Debug, Serialize, Deserialize)]
pub struct Rumor<T> {
    pub msg: T,
}
impl<T> Rumor<T>
    where T: Serialize
{
    pub fn new(r: T) -> Rumor<T> {
        Rumor {
            msg: r,
        }
    }
}
impl<T> FramedMessage for Rumor<T>
    where T: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::Rumor, self.msg)
    }

    fn msg_type() -> MessageType { MessageType::Rumor }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RumorResponse {
    same: bool,
    slow: bool,
}
impl RumorResponse {
    pub fn new(agree: bool, slow: bool) -> RumorResponse {
        RumorResponse {
            same: agree,
            slow: slow,
        }
    }

    pub fn agree(&self) -> bool { self.same }
    pub fn stale(&self) -> bool { self.slow }
}
impl FramedMessage for RumorResponse {
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::RumorResponse, self)
    }

    fn msg_type() -> MessageType { MessageType::RumorResponse }
}


//
// advertise
//

#[derive(Debug, Serialize, Deserialize)]
pub struct Advertise<T> {
    pub unique: T,
}
impl<T> Advertise<T>
    where T: Serialize
{
    pub fn new(r: T) -> Advertise<T> {
        Advertise {
            unique: r,
        }
    }

    pub fn key(&self) -> &T {
        &self.unique
    }
}
impl<T> FramedMessage for Advertise<T>
    where T: Serialize
{
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::Advertise, self)
    }

    fn msg_type() -> MessageType { MessageType::Advertise }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdvertiseResponse {
    has: bool,
}
impl AdvertiseResponse {
    pub fn new(has: bool) -> AdvertiseResponse {
        AdvertiseResponse {
            has: has,
        }
    }

    pub fn needs_rumor(&self) -> bool { !self.has }
}
impl FramedMessage for AdvertiseResponse {
    fn to_msg(self, clock: proto::RequestId) -> Message {
        Message::new(clock, MessageType::AdvertiseResponse, self)
    }

    fn msg_type() -> MessageType { MessageType::AdvertiseResponse }
}
