use bytes::{BytesMut, BufMut};

use futures::{future, Future, BoxFuture};

use tokio_core::net::TcpStream;
use tokio_io::{AsyncRead};
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_service::Service;
use tokio_proto::pipeline;

use serde::{Serialize, Deserialize};
use bincode;

use std::net;
use std::cmp::max;
use std::{io, str, fmt};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc};
use std::cell::Cell;

#[macro_use]
pub mod msg;
pub use self::msg::*;

pub mod challenge;

use App;
use UniqueProvider;
use peer;
use error::Error;
use node::{LocalInfo};
use client::{RemoteInfo};



//------------------------------------------------
//
// codec
//
//------------------------------------------------

// TODO: allow these to be configurable.... somehow.
pub const ENCODE_BUF_THRESHOLD: usize = (1024 * 1024) * 2;
pub const ENCODE_BUF_SIZE: usize = (1024 * 1024) * 10; // TODO: configurable


pub struct RumorCodec;

impl Encoder for RumorCodec {
    type Error = io::Error;
    type Item = Message;
    
    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        let hdr_len = Header::serialized_size();
        let needs = hdr_len + msg.hdr.data_len; // TODO: check u64 vs usize
        let grow = max(needs as usize, ENCODE_BUF_SIZE);

        let mut cap = buf.remaining_mut();
        if cap < needs as usize {
            let diff = grow - cap;
            trace!("growing encode buffer by: {}", diff);
            buf.reserve(diff);
            cap = buf.capacity();
        }

        let writer = &mut buf.writer();
        let r = bincode::serialize_into(writer, &msg.hdr, bincode::Bounded(hdr_len));
        if r.is_err() {
            info!("tried to write {} bytes into buffer of {}", hdr_len, cap);
            return Err(Error::serde(Some("could not serialize message header".to_string()), r.err().unwrap()).into());
        }
        
        writer.write_all(&msg.payload)
    }
}

// TODO TODO TODO TODO TODO TODO:
//     where do errors from decode go?
//     if you panic, the server crashes (expectedly), but returning
//     error seems to just blackhole the message....
//     needs to disconnect the client (which fails the Client::send())
//     or report the error, fulfilling the promise with said error
impl Decoder for RumorCodec {
    type Error = io::Error;
    type Item = Message;
    
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        let hdr_len = Header::serialized_size();
        if buf.len() < hdr_len as usize {
            return Ok(None);
        }

        let hdr: Header = match bincode::deserialize(&buf[0..hdr_len as usize]) {
            Err(e) => { return Err(Error::serde(Some("could not serialize header".to_string()), e).into()); }
            Ok(h) => { h }
        };

        // check if payload is here
        if buf.len() < (hdr_len + hdr.data_len) as usize {
            trace!("needs {} bytes, have {}", (hdr_len + hdr.data_len), buf.len());
            return Ok(None);
        }

        // split the header and payload
        buf.split_to(hdr_len as usize);
        let data = buf.split_to(hdr.data_len as usize).to_vec();

        Ok(Some(Message{hdr:hdr, payload:Arc::new(data)}))
    }
}


//------------------------------------------------
//
// client/server proto
//
//------------------------------------------------

pub struct RumorProto;

impl pipeline::ServerProto<TcpStream> for RumorProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<TcpStream, RumorCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        io.set_recv_buffer_size((1024 * 1204) * 10) ?; // TODO: make this configurable, 10 MiB
        io.set_send_buffer_size((1024 * 1204) * 1) ?; // TODO: make this configurable, 1 MiB
        Ok(io.framed(RumorCodec))
    }
}

impl pipeline::ClientProto<TcpStream> for RumorProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<TcpStream, RumorCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        io.set_recv_buffer_size((1024 * 1204) * 1) ?; // TODO: make this configurable, 1 MiB
        io.set_send_buffer_size((1024 * 1204) * 10) ?; // TODO: make this configurable, 10 MiB
        Ok(io.framed(RumorCodec))
    }
}

pub type Request = Message;
pub type Response = Message;


//------------------------------------------------
//
// middleware service
//
//------------------------------------------------

// TODO: error if any non-handshakey message comes before handshake complete
pub struct GossipService<I, S, C>
    where S: App,
          C: Send + Sync + Clone + challenge::Solver + 'static,
          I: Send + Sync + Clone + Copy + Ord + Serialize + fmt::Debug + 'static,
          Error: From<<C as challenge::Solver>::Error>,
          for<'de> I: Deserialize<'de>,
{
    local: Arc<LocalInfo<I>>,
    remote: Cell<RemoteInfo<I>>,
    app: S,
    peers: peer::PeersHandle<I, C>,
}
impl<I, S, C> GossipService<I, S, C>
    where I: Send + Sync + Clone + Copy + Ord + Serialize + fmt::Debug + 'static,
          S: App,
          C: Send + Sync + Clone + challenge::Solver + 'static,
          Error: From<<C as challenge::Solver>::Error>,
          for<'de> I: Deserialize<'de>
{
    pub fn new(local: Arc<LocalInfo<I>>, handle: peer::PeersHandle<I, C>, up: S) -> GossipService<I, S, C> {
        GossipService{
            local: local,
            remote: Cell::new(RemoteInfo::<I>::empty()),
            app: up,
            peers: handle,
        }
    }

    // TODO: ideally a const fn
    #[inline]
    pub fn null_addr() -> SocketAddr {
        let null_ip = net::IpAddr::V4(net::Ipv4Addr::new(0, 0, 0, 0));
        SocketAddr::new(null_ip, 0)
    }

    fn handshake_response(&self) -> HandshakeResponse<I> {
        // TODO: no expect/panic here
        let rem = self.remote.get();
        let rem_id = rem.id.as_ref().expect("did not get handshake ID");
        let rem_addr = rem.addr.as_ref().expect("did not get handshake address");
        let rem_type = rem.ty.as_ref().expect("did not set client type in handshake");

        // check for ourself, and a null ip
        // TODO: kill connection if so?
        let invalid_remote = rem_addr == &self.local.addr || rem_addr == &GossipService::<I,S,C>::null_addr();

        if !invalid_remote && self.peers.should_peer(Some(rem_id), rem_addr, Some(rem_type)) {
            // TODO: assert non-null address
            debug!("{:?} attempting to mutually peer with: {:?}", self.local.id, rem_addr);
            self.peers.mutual(*rem_addr);
        }


        let peers = self.peers.offered_seeds(rem_addr);
        debug!("node {:?} sending peers in handshake with {:?}: {:?}", self.local.id, rem_addr, peers);
        HandshakeResponse::new(self.local.id, peers)
    }

    pub fn intercept(&self, req: &Request) -> Option<BoxFuture<Response, Error>> {

        match req.hdr.msg_type {
            // handshake and send a challenge
            MessageType::Handshake => {
                let shake = match req.deframe::<Handshake<I>>() {
                    Err(e) => { return Some(future::err(e.into()).boxed()); }
                    Ok(s) => { s }
                };
                debug!("{:?} intercepted handshake from: {:?}, as type {:?}", self.local.id, shake.id(), shake.ty());

                // set the remote state we just received
                let mut rem = self.remote.get();
                rem.id = Some(*shake.id());
                rem.ty = Some(*shake.ty());
                rem.addr = Some(*shake.route());
                self.remote.set(rem);

                if let Some(chal) = self.app.generate() {
                    let rsp = Challenge::new(chal).to_msg(req.hdr.clock);
                    return Some(future::ok(rsp).boxed());

                }

                let rsp = self.handshake_response().to_msg(req.hdr.clock);
                Some(future::ok(rsp).boxed())
            }

            // wait for a challenge response
            MessageType::ChallengeResponse => {
                let chal = match req.deframe::<ChallengeResponse<<S as challenge::Challenger>::Response>>() {
                    Err(e) => { return Some(future::err(e.into()).boxed()); }
                    Ok(s) => { s }
                };

                if self.app.check(chal.answer) {
                    let shake = self.handshake_response();
                    return Some(future::ok(shake.to_msg(req.hdr.clock)).boxed());
                }

                Some(future::err(Error::new("failed challenge".to_string())).boxed())
            }

            _ => { None }
        }
    }

    pub fn handle_rumor(
        &self,
        rsp: &RumorResponse,
        key: <S::Rumor as UniqueProvider>::Unique,
        payload: Arc<Vec<u8>>
    )
        -> Result<(), BoxFuture<Response, Error>>
    {
        if rsp.stale() {
            trace!("intercepted stale rumor response: {:?}", rsp);
        } else {
            // TODO: check runtime cost of having both
            debug!("broadcasting rumor");
            self.peers.broadcast(key, payload);
        }

        Ok(())
    }
}

macro_rules! try_handle {
    ($handle:expr) => {{
        if let Err(e) = $handle { return e; }
    }}
}

impl<I, S, C> Service for GossipService<I, S, C>
    where I: Clone + Copy + Send + Sync + Ord + Serialize + fmt::Debug + 'static,
          S: App,
          C: Send + Sync + Clone + challenge::Solver + 'static,
          Error: From<<C as challenge::Solver>::Error>,
          for<'de> I: Deserialize<'de>
{
    type Request =  Request;
    type Response = Response;
    type Error =  Error;
    type Future = BoxFuture<Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        // check if this service should service the request (handshakes essentially)
        if let Some(r) = self.intercept(&req) { return r; }


        if req.hdr.msg_type == MessageType::Advertise {
            let adv = match req.deframe::<Advertise<<S::Rumor as UniqueProvider>::Unique>>() {
                Err(e) => {
                    error!("received invalid rumor advertise frame: {}", e);
                    return future::err(e).boxed();
                }
                Ok(r) => { r }
            };

            match self.app.has_unique(adv.key()).wait() {
                Err(e) => {
                    error!("application error checking for unique: {}", e);
                    return future::err(e).boxed();
                }
                Ok(h) => {
                    let adv_rsp = AdvertiseResponse::new(h);
                    return future::ok(adv_rsp.to_msg(req.hdr.clock)).boxed();
                }
            }
        }


        let rum = match req.deframe::<Rumor<S::Rumor>>() {
            Err(e) => {
                error!("received invalid rumor frame: {}", e);
                return future::err(e).boxed();
            }
            Ok(r) => { r }
        };


        let payload = req.payload.clone(); // Arc clone
        let unique = rum.msg.get_unique().clone(); // TODO: can we do this without clone?

        let result = self.app.handle(rum.msg).wait();
        if result.is_err() {
            // TODO: consider hanging up, karma, etc
            return future::err::<Response, Error>(result.err().unwrap()).boxed();
        }

        let rsp = result.unwrap();
        {
            try_handle!(self.handle_rumor(&rsp, unique, payload))
        }

        future::ok::<Response, Error>(rsp.to_msg(req.hdr.clock)).boxed()
    }
}
