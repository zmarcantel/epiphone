use tokio_proto::TcpServer;

use serde::{Serialize, Deserialize};

use std::{fmt};
use std::sync::Arc;
use std::net::SocketAddr;

use App;
use peer;
use proto;
use proto::challenge;
use error::Error;



#[derive(Clone)]
pub struct LocalInfo<I>
    where I: Clone + Sync + Send + Serialize
{
    pub id: I,
    pub addr: SocketAddr,
}
impl<I> LocalInfo<I>
    where I: Clone + Sync + Send + Serialize
{
    pub fn new(id: I, addr: SocketAddr) -> LocalInfo<I> {
        LocalInfo{
            addr: addr,
            id: id,
        }
    }
}


#[derive(Clone)]
pub struct ServiceInfo<S>
    where S: challenge::Solver
{
    pub solver: S,
    pub seed_storage: usize,
    pub initial_seeds: Vec<SocketAddr>,
    pub broadcast_buffer: usize,
}
impl<S> ServiceInfo<S>
    where S: challenge::Solver
{
    pub fn new(solve: S, seeds: Vec<SocketAddr>, seed_store: usize, broad_buff: usize) -> ServiceInfo<S> {
        ServiceInfo{
            solver: solve,
            seed_storage: seed_store,
            initial_seeds: seeds,
            broadcast_buffer: broad_buff,
        }
    }
}


#[derive(Clone)]
pub struct Config<I, C>
    where I: Clone + Sync + Send + Serialize,
          C: challenge::Solver,
{
    pub local: LocalInfo<I>,
    pub service: ServiceInfo<C>,
}
impl<I, C> Config<I, C>
    where I: Clone + Sync + Send + Serialize,
          C: challenge::Solver,
{
    pub fn new(id: I, bind: SocketAddr, seeds: Vec<SocketAddr>, peer_sz: usize, broad_buff: usize, solve: C) -> Config<I, C> {
        Config{
            local: LocalInfo::new(id, bind),
            service: ServiceInfo::new(solve, seeds, peer_sz, broad_buff),
        }
    }

    pub fn from(local: LocalInfo<I>, serv: ServiceInfo<C>) -> Config<I, C> {
        Config{
            local: local,
            service: serv,
        }
    }
}

// TODO: we clone the id _alot_ and it all begins here
pub struct Node<I, C>
    where I: Send + Sync + Ord + Clone + Copy + Serialize + fmt::Debug + 'static,
          C: Send + Sync + Clone + challenge::Solver + 'static,
          Error: From<<C as challenge::Solver>::Error>,
          for<'de> I: Deserialize<'de>,
{
    local: Arc<LocalInfo<I>>,
    service: Arc<ServiceInfo<C>>,
    peers: peer::PeersHandle<I, C>,
}
impl<I, C> Node<I, C>
    where I: Serialize + fmt::Debug + Clone + Copy + Send + Sync + Ord,
          C: challenge::Solver + Send + Sync + Clone, // TODO: why does this need sync?
          Error: From<<C as challenge::Solver>::Error>,
          for<'de> I: Deserialize<'de>
{
    pub fn new(conf: Config<I, C>) -> Node<I, C> {
        let arc_local = Arc::new(conf.local);
        let arc_service = Arc::new(conf.service);
        let p = peer::PeerManager::new(arc_local.clone(), arc_service.clone());
        Node {
            local: arc_local,
            service: arc_service,
            peers: p.handle(),
        }
    }

    pub fn local_info(&self) -> Arc<LocalInfo<I>> {
        self.local.clone()
    }

    pub fn service_info(&self) -> Arc<ServiceInfo<C>> {
        self.service.clone()
    }

    // TODO: custom error in future
    pub fn start<S>(&mut self, service: S)
        where I: 'static,
              C: 'static,
              S: App + 'static,
    {
        let server = TcpServer::new(proto::RumorProto, self.local.addr);
        let handle = self.peers.clone();

        for s in &self.service.initial_seeds {
            handle.connect(*s);
        }

        let local = self.local.clone();
        server.serve(move || {
            Ok(proto::GossipService::new(local.clone(), handle.clone(), service.clone()))
        })
    }
}
