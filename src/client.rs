use futures::{future, Future, BoxFuture};
use futures::sync::oneshot;

use tokio_core::net;
use tokio_core::reactor;
use tokio_proto::TcpClient;
use tokio_proto::pipeline::{ClientService};
use tokio_service::Service;

use serde::{Serialize, Deserialize};

use std::{fmt};
use std::thread;
use std::sync::Arc;
use std::net::SocketAddr;

use proto;
use error::Error;
use proto::challenge;
use node::{LocalInfo, ServiceInfo};


#[derive(Clone, Copy)] // TODO: would rather not, but Cell
pub struct RemoteInfo<I>
    where I: Serialize
{
    pub addr: Option<SocketAddr>,
    pub id: Option<I>,
    pub ty: Option<proto::ClientType>,
}
impl<I> RemoteInfo<I>
    where I: Serialize
{
    pub fn new(addr: SocketAddr) -> RemoteInfo<I> {
        RemoteInfo{
            addr: Some(addr),
            id: None,
            ty: None,
        }
    }

    pub fn full(addr: SocketAddr, id: I, ty: proto::ClientType) -> RemoteInfo<I> {
        RemoteInfo{
            addr: Some(addr),
            id: Some(id),
            ty: Some(ty),
        }
    }

    pub fn empty() -> RemoteInfo<I> {
        RemoteInfo{
            addr: None,
            id: None,
            ty: None,
        }
    }
}


pub type ResponseResult = Result<proto::Response, Error>;
pub type ResponseFuture = oneshot::Receiver<ResponseResult>;

pub struct Config<I, S>
    where S: proto::challenge::Solver,
          I: Clone + Copy + Send + Sync + Serialize + fmt::Debug,
{
    pub local: Arc<LocalInfo<I>>,
    pub service: Arc<ServiceInfo<S>>,

    pub local_type: proto::ClientType,

    pub remote: RemoteInfo<I>,
}
impl<I, S> Config<I, S>
    where S: proto::challenge::Solver,
          I: Clone + Copy + Send + Sync + Serialize + fmt::Debug,
{
    pub fn new(local: LocalInfo<I>, service: ServiceInfo<S>, rem: SocketAddr) -> Config<I, S> {
        Config {
            local: Arc::new(local),
            service: Arc::new(service),

            local_type: proto::ClientType::Publisher,

            remote: RemoteInfo::<I>::new(rem),
        }
    }

    #[inline]
    pub fn publisher_from(local: LocalInfo<I>, service: ServiceInfo<S>, rem: SocketAddr) -> Config<I, S> {
        Config::<I, S>::publisher(Arc::new(local), Arc::new(service), rem)
    }

    pub fn publisher(local: Arc<LocalInfo<I>>, service: Arc<ServiceInfo<S>>, rem: SocketAddr) -> Config<I, S> {
        Config {
            local: local,
            service: service,

            local_type: proto::ClientType::Publisher,

            remote: RemoteInfo::<I>::new(rem),
        }
    }


    #[inline]
    pub fn listener_from(local: LocalInfo<I>, service: ServiceInfo<S>, rem: SocketAddr) -> Config<I, S> {
        Config::<I, S>::listener(Arc::new(local), Arc::new(service), rem)
    }
    pub fn listener(local: Arc<LocalInfo<I>>, service: Arc<ServiceInfo<S>>, rem: SocketAddr) -> Config<I, S> {
        Config {
            local: local,
            service: service,

            local_type: proto::ClientType::Listener,

            remote: RemoteInfo::<I>::new(rem),
        }
    }


    #[inline]
    pub fn full_from(local: LocalInfo<I>, service: ServiceInfo<S>, rem: SocketAddr) -> Config<I, S> {
        Config::<I, S>::full(Arc::new(local), Arc::new(service), rem)
    }
    pub fn full(local: Arc<LocalInfo<I>>, service: Arc<ServiceInfo<S>>, rem: SocketAddr) -> Config<I, S> {
        Config {
            local: local,
            service: service,

            local_type: proto::ClientType::Full,

            remote: RemoteInfo::<I>::new(rem),
        }
    }
}


pub struct Client<I, S>
    where S: Send + Sync + proto::challenge::Solver,
          I: Clone + Copy + Send + Sync + Serialize + fmt::Debug,
{
    conf: Config<I, S>,

    remote: Option<reactor::Remote>,
    tcp: ClientService<net::TcpStream, proto::RumorProto>,
    clock: proto::RequestId,
}

impl<I, S> Client<I, S>
    where I: Clone + Copy + Send + Sync + Serialize + fmt::Debug + 'static,
          S: proto::challenge::Solver + Send + Sync + 'static,
          <S as challenge::Solver>::Challenge: Serialize + fmt::Debug,
          <S as challenge::Solver>::Response: Serialize + fmt::Debug,
          <S as challenge::Solver>::Error: Serialize + fmt::Debug,
          for<'de> I: Deserialize<'de>,
          for<'de> <S as challenge::Solver>::Challenge: Deserialize<'de>,
          for<'de> <S as challenge::Solver>::Response: Deserialize<'de>,
          for<'de> <S as challenge::Solver>::Error: Deserialize<'de>,
{
    pub fn id(&self) -> &I {
        &self.conf.local.id
    }

    pub fn peer(&self) -> &Option<I> {
        &self.conf.remote.id
    }

    pub fn peer_addr(&self) -> &Option<SocketAddr> {
        &self.conf.remote.addr
    }

    pub fn route(&self) -> &SocketAddr {
        &self.conf.local.addr
    }

    pub fn connect(conf: Config<I, S>) -> BoxFuture<(Client<I, S>, proto::HandshakeResponse<I>), Error> {
        // create the client worker thread
        // TODO: how to kill this thread on client disconnect (could be a broadcast client).
        //       seems you could crate a "thread bomb" by causing lots of connections and defunct threads
        let (core_tx, core_rx) = oneshot::channel();
        thread::Builder::new()
            .name(format!("client-core-{}", conf.remote.addr.as_ref().expect("unknown remote address")))
            .spawn(move || {
                let mut core = reactor::Core::new().expect("could not create Client Core");
                let remote = core.remote();
                core_tx.send(remote).expect("could not send Remote Core");
                loop {
                    core.turn(None);
                }
            }).expect("could not make client-bound thread");

        // spawn the connection into the Core in the above thread
        let (cli_tx, cli_rx) = oneshot::channel();
        let remote = core_rx.wait().expect("could not receive Remote Core");
        let tmp_remote = remote.clone();

        tmp_remote.spawn(move |handle| {
            TcpClient::new(proto::RumorProto)
                .connect(conf.remote.addr.as_ref().expect("unknown remote address"), handle)
                .then(move |service| {
                    match service {
                        Err(e) => {
                            let send_res = cli_tx.send(Err(e));
                            if send_res.is_err() {
                                // TODO: can we pass error without panic?
                                panic!("broken Client result channel for error");
                            }
                          Err(())
                        }
                        Ok(s) => {
                            trace!("got service");
                            let send_res = cli_tx.send(Ok(Client {
                                conf: conf,
                                remote: Some(remote),
                                tcp: s,
                                clock: 0,
                            }));
                            if send_res.is_err() {
                                // TODO: can we pass error without panic?
                                panic!("broken Client result channel");
                            }
                            Ok(())
                        }
                    }
                })
        });

        cli_rx
            .map_err(|e| {  e.into()  })
            .and_then(move |cli| {
                match cli {
                    Ok(client) => {
                        client.handshake()
                    }
                    Err(e) => {
                        future::err(Error::io( // TODO: IntoFuture
                            Some("error response for Client in success future".to_string()), e
                        )).boxed()
                    }
                }
            })
            .boxed()
    }

    fn handshake(mut self) -> BoxFuture<(Client<I, S>, proto::HandshakeResponse<I>), Error> {
        let id = self.conf.local.id;
        let msg = match self.conf.local_type {
            proto::ClientType::Publisher => { proto::Handshake::publisher(id, self.conf.local.addr) }
            proto::ClientType::Listener => { proto::Handshake::listener(id, self.conf.local.addr) }
            proto::ClientType::Full => { proto::Handshake::new(id, self.conf.local.addr) }
        };
        debug!("id {:?} sending handshake from {:?} to {:?}", msg.id(), msg.route(), self.conf.remote.addr);

        self.send(msg)
            .map_err(|e| {  e.into()  })
            .and_then(move |result| {
                if result.is_err() {
                    return future::err(result.err().unwrap()).boxed();
                }

                let rsp = result.unwrap();

                // TODO: this could use some cleanup / simplification
                match rsp.hdr.msg_type {
                    proto::MessageType::HandshakeResponse => {
                        let shake = match rsp.deframe::<proto::HandshakeResponse<I>>() {
                            Err(e) => { return future::err(e.into()).boxed(); }
                            Ok(s) => { s }
                        };
                        self.handle_handshake_rsp(&shake);
                        future::ok((self, shake)).boxed()

                    }

                    proto::MessageType::Challenge => {
                        let puzzle = match rsp.deframe::<proto::Challenge<<S as challenge::Solver>::Challenge>>() {
                            Err(e) => { return future::err(e.into()).boxed(); }
                            Ok(p) => { p }
                        };
                        self.handle_challenge(puzzle)
                    }

                    _ => {
                        future::err(Error::unexpected(
                            vec!(proto::MessageType::HandshakeResponse), rsp.hdr.msg_type,
                            "could not handshake".to_string()
                        )).boxed()
                    }
                }

            })
            .boxed()
    }

    fn handle_challenge(mut self, chal: proto::Challenge<<S as challenge::Solver>::Challenge>)
        -> BoxFuture<(Client<I, S>, proto::HandshakeResponse<I>), Error>
    {
        trace!("got handshake challenge: {:?}", chal);
        let solved = match self.conf.service.solver.solve(chal.puzzle) {
            Ok(s) => { s }
            Err(e) => {
                return future::err(Error::solve_err(format!("{:?}", e))).boxed()
            }
        };
        trace!("solved challenge: {:?}", solved);

        self.send(proto::ChallengeResponse::new(solved))
            .map_err(|_| {
                Error::new("future cancelled".to_string()) // TODO: better error descriptor than Generic
            })
            .and_then(move |chal_rsp| {
                match chal_rsp {
                    Err(_) => {
                        future::err(Error::io_other("hung up during handshake challenge".to_string())).boxed()
                    }
                    Ok(r) => {
                        let shake = match r.deframe::<proto::HandshakeResponse<I>>() {
                            Err(e) => { return future::err(e.into()).boxed(); }
                            Ok(p) => { p }
                        };
                        self.handle_handshake_rsp(&shake);
                        future::ok((self, shake)).boxed()
                    }
                }
            })
            .boxed()
    }

    fn handle_handshake_rsp(&mut self, shake: &proto::HandshakeResponse<I>) {
        debug!("handshake complete: {:?}", shake);
        self.conf.remote.id = Some(*shake.id());
        // TODO: what about remote type? think it's unneeded here
        if shake.seeds().is_empty() {
            debug!("{:?} was notified of peers: {:?}", self.id(), shake.seeds());
        }
    }

    pub fn send<T>(&mut self, req: T) -> ResponseFuture
        where T: proto::FramedMessage
    {
        let msg = req.to_msg(self.clock);
        self.clock += 1;
        self.send_msg(msg, false)
    }


    pub fn send_msg(&mut self, mut req: proto::Message, set_clock: bool) -> ResponseFuture {
        trace!("sending: {:?}", req);

        if set_clock {
            req.hdr.clock = self.clock;
            self.clock += 1;
        }

        let (tx, rx) = oneshot::channel();
        let remote = self.remote.take().expect("use of uninitialized Remote");
        let fut = self.tcp.call(req);
        remote.spawn(|_| {
            fut.then(move |r| {
                if r.is_err() { // TODO: do something with the error
                    trace!("error in request: {:?}", r);
                } else {
                    trace!("got response: {:?}", r);
                }
                let send_res = tx.send(r.map_err(|e| e.into()));
                if send_res.is_err() {
                    // TODO: possible to communicate error without panic?
                    panic!("response channel hung up");
                }
                future::ok(())
            })
        });
        self.remote = Some(remote);
        rx
    }
}
