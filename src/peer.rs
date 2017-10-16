use futures::{future, Future, stream, Stream, Poll, Async, Canceled};
use futures::sync::{oneshot};

use tokio_core::reactor;

use serde::{Serialize, Deserialize};

use std::fmt;
use std::thread;
use std::net::SocketAddr;
use std::sync::{Arc,Mutex};
use std::collections::{btree_map, BTreeMap, HashSet, VecDeque};

use client;
use proto;
use proto::challenge;
use error::Error;
use node::{LocalInfo, ServiceInfo};

pub type BroadcastResult<I> = (I, client::ResponseResult);

pub struct BroadcastFuture<I> {
    pub from: I,
    pub fut: client::ResponseFuture,
}
impl<I> BroadcastFuture<I> {
    pub fn new(id: I, fut: client::ResponseFuture) -> BroadcastFuture<I> {
        BroadcastFuture{
            from: id,
            fut: fut,
        }
    }
}
impl<I> Future for BroadcastFuture<I>
    where I: Clone
{
    type Item = BroadcastResult<I>;
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::Ready(r)) => {
                Ok(Async::Ready((self.from.clone(), r))) // TODO: avoid yet another clone
            }

            Ok(Async::NotReady) => {
                Ok(Async::NotReady)
            }

            Err(e) => {
                Err(e)
            }
        }
    }
}


pub struct Peer<I, S>
    where I: Send + Sync + Clone + Copy + Serialize + fmt::Debug,
          S: Send + Sync + challenge::Solver,
          for<'de> I: Deserialize<'de>,
{
    pub cli: client::Client<I, S>,
    pub stale: usize,
}
impl<I, S> Peer<I, S>
    where I: Send + Sync + Clone + Copy + Serialize + fmt::Debug + 'static,
          S: Send + Sync + challenge::Solver + 'static,
          for<'de> I: Deserialize<'de>,
{
    pub fn new(cli: client::Client<I,S>) -> Peer<I,S> {
        Peer {
            cli: cli,
            stale: 0usize,
        }
    }

    pub fn send_msg(&mut self, req: proto::Message, set_clock: bool)
        -> client::ResponseFuture
    {
        self.cli.send_msg(req, set_clock)
    }

    pub fn id(&self) -> &I {
        self.cli.id()
    }

    pub fn peer(&self) -> &Option<I> {
        self.cli.peer()
    }

    pub fn peer_addr(&self) -> &Option<SocketAddr> {
        self.cli.peer_addr()
    }

    pub fn route(&self) -> &SocketAddr {
        self.cli.route()
    }
}


// TODO: have this hold the local routing address rather than services/clients if we can
pub struct PeerManager<I, S>
    where I: Send + Sync + Clone + Copy + Serialize + fmt::Debug,
          S: Send + Sync + challenge::Solver,
          for<'de> I: Deserialize<'de>,
{
    local: Arc<LocalInfo<I>>,
    service: Arc<ServiceInfo<S>>,
    core_remote: reactor::Remote,

    // TODO: clearly need faster structures, but need access pattern data first
    fast: BTreeMap<I, Peer<I, S>>,
    slow: BTreeMap<I, Peer<I, S>>,
    pending: HashSet<SocketAddr>,
    known: VecDeque<SocketAddr>, // TODO: manage this list
}
impl<I, S> PeerManager<I, S>
    where I: Send + Sync + Ord + Clone + Copy + Serialize + fmt::Debug + 'static,
          S: Send + Sync + Clone + challenge::Solver + 'static,
          for<'de> I: Deserialize<'de>,
{
    pub fn new(local: Arc<LocalInfo<I>>, service: Arc<ServiceInfo<S>>) -> PeerManager<I, S> {
        let store_sz = service.seed_storage;

        // create the broadcast worker thread
        let (core_tx, core_rx) = oneshot::channel();
        thread::Builder::new()
            .name(format!("peer-core-{:?}", local.id))
            .spawn(move || {
                let mut core = reactor::Core::new().expect("could not create peer manager Core");
                let remote = core.remote();
                core_tx.send(remote).expect("could not send Remote Core");
                loop {
                    core.turn(None);
                }
            }).expect("could not make peer-manager-bound thread");

        // spawn the connection into the Core in the above thread
        let remote_core = core_rx.wait().expect("could not receive peerman Remote Core");

        PeerManager {
            local: local,
            service: service,
            core_remote: remote_core,

            fast: BTreeMap::new(),
            slow: BTreeMap::new(),
            pending: HashSet::new(),
            known: VecDeque::with_capacity(store_sz),
        }
    }

    pub fn handle(self) -> PeersHandle<I, S> { PeersHandle(Arc::new(Mutex::new(self))) }

    pub fn num_fast(&self) -> usize { self.fast.len() }
    pub fn num_slow(&self) -> usize { self.slow.len() }
    pub fn num_pending(&self) -> usize { self.pending.len() }
    pub fn num_inactive(&self) -> usize { self.known.len() }
    pub fn total(&self) -> usize { self.num_fast() + self.num_slow() + self.num_pending() + self.num_inactive() }

    pub fn push_active(&mut self, c: client::Client<I, S>) {
        let remote_id = *c.peer().as_ref().expect("client remote ID not set during handshake");
        self.pending.remove(c.route());
        assert!(self.fast.insert(remote_id, Peer::new(c)).is_none()); // TODO: better error handling here
    }

    // TODO: which seeds we pass needs to be experimented with
    // TODO: lots of expects
    pub fn offered_seeds(&self, remote: &SocketAddr) -> Vec<SocketAddr> {
        let mut result = vec!();
        for f in &self.fast {
            let stored_rem = f.1.peer_addr().as_ref().expect("do not know address of a connected eager-client");
            if stored_rem == remote { continue; }
            result.push(*stored_rem);
        }

        for f in &self.slow {
            let stored_rem = f.1.peer_addr().as_ref().expect("do not know address of a connected lazy-client");
            if stored_rem == remote { continue; }
            result.push(*stored_rem);
        }

        result
    }


    // TODO: lots of expects
    pub fn has(&self, id: Option<&I>, addr: Option<&SocketAddr>, include_pend: bool) -> bool {
        if let Some(i) = id {
            if self.fast.contains_key(i) { return true; }
            if self.slow.contains_key(i) { return true; }
        }

        if let Some(a) = addr{
            if include_pend && self.pending.contains(a) { return true; }

            for s in &self.fast {
                if s.1.peer_addr().as_ref().expect("unset eager-client remote address") == a { return true; }
            }

            for s in &self.slow {
                if s.1.peer_addr().as_ref().expect("unset lazy-client remote address") == a { return true; }
            }

            for k in &self.known {
                if k == a { return true; }
            }
        }

        false
    }

    fn make_lazy(&mut self, cid: I) -> bool {
        // TODO TODO
        if let Some(curr) = self.fast.get_mut(&cid) {
            curr.stale += 1;
            if curr.stale < 1 { // TODO: config
                return false;
            }
        } // TODO: this is a lookup before a lookup below

        if let Some(c_move) = self.fast.remove(&cid) {
            self.slow.insert(cid, c_move);
            debug!("after removing {:?}, node {:?} has {} eager peers",
                cid, self.local.id, self.fast.len());
            true
        } else {
            false
        }
    }

    fn resend(&mut self, target: &I, payload: Arc<Vec<u8>>) {
        let reframed = proto::Message::from_heap(
            0u64, proto::MessageType::Rumor,
            payload.clone()
        );
        let fut = match self.slow.entry(*target) {
            btree_map::Entry::Occupied(mut cli) => {
                let f = cli.get_mut().send_msg(reframed, true);
                // ... and make eager
                let (k, v) = cli.remove_entry();
                self.fast.insert(k, v);
                f
            }

            btree_map::Entry::Vacant(_) => {
                match self.fast.get_mut(target) {
                    Some(cli) => {
                        cli.send_msg(reframed, true)
                    }

                    None => {
                        // TODO
                        error!("attempted to retransmit to a disconnected peer");
                        return;
                    }
                }
            }
        };

        self.core_remote.spawn(move |_| {
            fut.map_err(|e| { error!("advertise resend error: {}", e); () })
                .and_then(move |rsp| {
                debug!("advertise resend response: {:?}", rsp);
                if rsp.is_err() {
                    future::err(())
                } else {
                    future::ok(())
                }
            })
        });
    }

    // TODO: timeouts and other protections
    pub fn broadcast<K>(&mut self, key: K, payload: Arc<Vec<u8>>, handle: PeersHandle<I, S>)
        where K: Clone + Serialize,
              for<'de> K: Deserialize<'de>
    {
        let mut eag = stream::FuturesUnordered::new();
        for c in &mut self.fast {
            debug!("{:?} broadcasting to {:?}", self.local.id, c.0);
            let reframed = proto::Message::from_heap(
                0u64, proto::MessageType::Rumor,
                payload.clone()
            );
            eag.push(BroadcastFuture::new(*c.0, c.1.send_msg(reframed, true)));
        }

        let mut laz = stream::FuturesUnordered::new();
        for c in &mut self.slow {
            debug!("{:?} advertising to {:?}", self.local.id, c.0);
            let framed = proto::Message::new(
                0u64, proto::MessageType::Advertise, key.clone()
            );
            laz.push(BroadcastFuture::new(*c.0, c.1.send_msg(framed, true)));
        }

        let h = handle.clone(); // TODO: try to avoid, but it's "cheap"
        self.core_remote.spawn(move |_| {
            eag.for_each(move |(cid, rsp)| {
                // TODO: handle rsp as error

                let ack = match rsp.as_ref().unwrap().deframe::<proto::RumorResponse>() {
                    Ok(a) => { a }
                    Err(e) => {
                        // TODO: disconnect other side
                        info!("could not deframe broadcasted rumor response: {}", e);
                        return future::ok(());
                    }
                };

                if ack.stale() {
                    debug!("received stale broadcast response from {:?}, moving to lazy", cid);
                    handle.clone().make_lazy(cid);
                }

                future::ok(())
            })
            .map_err(|e| {
                // TODO TODO TODO
                error!("received error response for broadcast: {}", e);
                ()
            })
        });

        self.core_remote.spawn(move |_| {
            laz.for_each(move |(cid, rsp)| {
                // TODO: handle rsp as error

                let adv = match rsp.as_ref().unwrap().deframe::<proto::AdvertiseResponse>() {
                    Ok(a) => { a }
                    Err(e) => {
                        // TODO: disconnect other side
                        info!("could not deframe advertise rumor response: {}", e);
                        return future::ok(());
                    }
                };

                if adv.needs_rumor() {
                    h.clone().resend(&cid, payload.clone());
                }

                future::ok(())
            })
            .map_err(|e| {
                // TODO TODO TODO
                error!("received error response for re-broadcast: {}", e);
                ()
            })
        })
    }
}


// TODO: Mutex --> RwLock
#[derive(Clone)]
pub struct PeersHandle<I, S>(pub Arc<Mutex<PeerManager<I, S>>>)
    where I: Send + Sync + Clone + Copy + Ord + Serialize + fmt::Debug + 'static,
          S: Send + Sync + Clone + challenge::Solver + 'static,
          for<'de> I: Deserialize<'de>,
;
impl<I, S> PeersHandle<I, S>
    where I: Send + Sync + Clone + Copy + Ord + Serialize + fmt::Debug + 'static,
          S: Send + Sync + Clone + challenge::Solver + 'static,
          for<'de> I: Deserialize<'de>
{
    pub fn mutual(&self, rem_addr: SocketAddr) {
        let conf = {
            let g = self.0.lock().expect("poisoned peers lock");
            let local = &g.local;
            let service = &g.service;
            client::Config::publisher(local.clone(), service.clone(), rem_addr)
        };
        self.conn(conf);
    }

    pub fn connect(&self, rem_addr: SocketAddr) {
        let conf = {
            let g = self.0.lock().expect("poisoned peers lock");
            let local = &g.local;
            let service = &g.service;
            client::Config::full(local.clone(), service.clone(), rem_addr)
        };
        self.conn(conf);
    }

    pub fn offered_seeds(&self, remote: &SocketAddr) -> Vec<SocketAddr> {
        let guard = self.0.lock().expect("poisoned peers lock"); // TODO: panics
        guard.offered_seeds(remote)
    }

    pub fn has(&self, id: Option<&I>, addr: Option<&SocketAddr>, include_pend: bool) -> bool {
        let guard = self.0.lock().expect("poisoned peers lock"); // TODO: panics
        guard.has(id, addr, include_pend)
    }

    pub fn should_peer(&self, rem_id: Option<&I>, remote: &SocketAddr, rem_type: Option<&proto::ClientType>) -> bool {
        // TODO: consider connection limits first

        // avoid dupes
        if self.has(rem_id, Some(remote), true) {
            debug!("not peering due to dupe in peer list: {:?}/{:?}", rem_id, remote);
            return false;
        }

        if let Some(t) = rem_type {
            // TODO: less panic in expect
            match *t {
                proto::ClientType::Publisher => { false }

                proto::ClientType::Listener | proto::ClientType::Full => {
                    true
                }
            }
        } else {
            true
        }
    }

    pub fn get_core(&self) -> reactor::Remote {
        let guard = self.0.lock().expect("poisoned peers lock"); // TODO: expect
        guard.core_remote.clone()
    }

    fn make_lazy(&mut self, cid: I) -> bool {
        let mut guard = self.0.lock().expect("poisoned peers lock"); // TODO: expect
        guard.make_lazy(cid)
    }

    pub fn resend(&mut self, target: &I, payload: Arc<Vec<u8>>) {
        let mut guard = self.0.lock().expect("poisoned peers lock"); // TODO: expect
        guard.resend(target, payload)
    }

    // TODO: timeouts and other protections
    pub fn broadcast<K>(&self, key: K, payload: Arc<Vec<u8>>)
        where K: Clone + Serialize,
              for<'de> K: Deserialize<'de>
    {
        let mut guard = self.0.lock().expect("poisoned peers lock"); // TODO: expect
        guard.broadcast(key, payload, self.clone())
    }


    fn conn(&self, conf: client::Config<I, S>) {
        let dupe = self.clone();

        {
            let mut g = dupe.0.lock().expect("poisoned peers lock"); // TODO: expect
            g.pending.insert(*conf.remote.addr.as_ref().expect("do not know the address we will connect to"));
        }

        thread::spawn(move || {
            // TODO: less panic on lock and future
            client::Client::connect(conf)
                .and_then(|(cli, shake)| {
                    for s in shake.seeds().iter() {
                        if ! dupe.should_peer(None, s, None) { continue; }
                        dupe.connect(*s);
                    }

                    let mut guard = dupe.0.lock().expect("poisoned peer handle lock");

                    let peer_id = cli.peer().as_ref()
                        .cloned().expect("did not receive or set client during in handshake");
                    let peer_addr = cli.peer_addr().as_ref()
                        .cloned().expect("did not receive or set client address in handshake");
                    let node_id = *cli.id();

                    if guard.has(Some(&peer_id), Some(&peer_addr), false) {
                        info!("{:?} got duplicate connection for {:?}", node_id, cli.peer());
                        return future::err(Error::new(format!("duplicate connection {:?} @ {:}", peer_id, peer_addr)));
                    }

                    guard.push_active(cli);
                    info!("after handshaking with {:?}, node {:?} has {} eager peers", peer_id, node_id, guard.num_fast());

                    future::ok(())
                })
                .wait()
                .expect("failed to connect to peer");
        });
    }

}
