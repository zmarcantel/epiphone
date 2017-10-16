extern crate futures;
use futures::{future, Future, BoxFuture};

extern crate tokio_service;

extern crate tokio_proto;

#[macro_use]
extern crate serde_derive;
extern crate serde;
use serde::Serialize;

extern crate rand;
use rand::Rng;

extern crate time;
extern crate chrono; // TODO: remove?

extern crate epiphone;
use epiphone::client;
use epiphone::client::{Config, Client};
use epiphone::proto;
use epiphone::proto::challenge;

use std::{fmt};
use std::thread;
use std::time::Duration;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::BTreeMap;

//------------------------------------------------
//
// kv service
//
//------------------------------------------------

pub type KvKey = String;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvVal {
    pub val: String,
    pub ts:  chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvWrite {
    pub key: KvKey,
    pub val: KvVal,
}
impl KvWrite {
    pub fn new(k: KvKey, v: String, ts: chrono::DateTime<chrono::Utc>) -> KvWrite {
        KvWrite {
            key: k,
            val: KvVal{val:v, ts: ts},
        }
    }
}
impl epiphone::UniqueProvider for KvWrite {
    type Unique = KvKey;
    fn get_unique(&self) -> &Self::Unique {
        &self.key
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KvChallenge {
    pub nonce: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KvChallengeResponse {
    pub answer: u64,
}

#[derive(Clone)]
pub struct KvSolver;
impl challenge::Solver for KvSolver {
    type Challenge = KvChallenge;
    type Response = KvChallengeResponse;
    type Error = String;

    fn solve(&self, c: Self::Challenge) -> Result<Self::Response, Self::Error> {
        Ok(KvChallengeResponse{answer: c.nonce >> 1})
    }
}

#[derive(Clone, Default)]
pub struct KvDb {
    store: Arc<Mutex<BTreeMap<KvKey, KvVal>>>,
}
impl KvDb {
    pub fn new() -> KvDb {
        KvDb{store: Arc::new(Mutex::new(BTreeMap::new()))}
    }
}
impl std::ops::Deref for KvDb {
    type Target = Arc<Mutex<BTreeMap<KvKey, KvVal>>>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}


#[derive(Clone)]
pub struct KvService<I>
    where I: Clone + fmt::Debug + Serialize,
{
    id: I,
    db: KvDb,
    // NOTE: do not follow this pattern.... it would use the same nonce for every connection.
    //       that may be desired, but if not, be aware.
    challenge_nonce: u64,
}
impl<I> KvService<I>
    where I: Clone + fmt::Debug + Serialize,
{
    pub fn new(id: I, db: KvDb) -> KvService<I> {
        KvService {
            id: id,
            db: db,
            challenge_nonce: rand::thread_rng().gen::<u64>(),
        }
    }

    pub fn get(&self, key: KvKey) -> (KvKey, Option<KvVal>) {
        let val = self.db.lock().expect("poisoned DB lock").get(&key).cloned();
        (key, val)
    }

    pub fn handle_rumor(&self, req: KvWrite) -> Result<proto::RumorResponse, epiphone::Error> {
        let mut db = self.db.lock().unwrap();

        let (agree, stale) = match db.get(&req.key) {
            None => { (true, false) } // new, so agree but not stale
            Some(v) => {
                (
                    req.val.val == v.val, // agree
                    req.val.ts <= v.ts,   // stale
                )
            }
        };
        if ! stale {
            debug!("db accepted write: {:?}", req);
            db.insert(req.key, req.val);
        }

        Ok(proto::RumorResponse::new(agree, stale))
    }
}

impl<I> epiphone::App for KvService<I>
    where I: Clone + Send + Sync + fmt::Debug + Serialize,
{
    type Rumor = KvWrite;
    type Challenge = KvChallenge;
    type ChallengeResponse = KvChallengeResponse;

    fn has_unique(&self, key: &KvKey) -> BoxFuture<bool, epiphone::Error> {
        let db = self.db.lock().unwrap();
        future::ok(db.contains_key(key)).boxed()
    }

    fn handle(&self, rum: Self::Rumor) -> BoxFuture<epiphone::proto::RumorResponse, epiphone::Error> {
        let rsp = match self.handle_rumor(rum) {
            Err(e) => { return future::err(e).boxed(); }
            Ok(r) => { r }
        };
        future::ok(rsp).boxed()
    }
}

impl<I> challenge::Challenger for KvService<I>
    where I: Clone + fmt::Debug + Serialize,
{
    type Challenge = KvChallenge;
    type Response = KvChallengeResponse;

    fn generate(&self) -> Option<Self::Challenge> {
        Some(KvChallenge{nonce:self.challenge_nonce})
    }

    fn check(&self, r: Self::Response) -> bool {
        r.answer == (self.challenge_nonce >> 1)
    }
}


//------------------------------------------------
//
// simple test
//
//------------------------------------------------

#[test]
fn simple() {
    setup_logging();

    let num_ops: usize = 50_000;
    let key_len = 64;
    let val_len = 0x1000; // 4 KiB
    let mut rsp_count = 0;

    let base_port: u16 = 8800;
    let num_nodes: usize = 4;
    let localhost = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let null_ip = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let null_addr = SocketAddr::new(null_ip, 0);

    let node_confs: Vec<epiphone::node::Config<usize, KvSolver>> = (0..num_nodes)
        .into_iter()
        .map(|i| {
            let solve = KvSolver{};
            let bind_addr = SocketAddr::new(localhost, base_port + (i as u16));
            let prev_node = if i > 0 {
                    vec!(SocketAddr::new(localhost, base_port + ((i-1) as u16)))
                } else {
                    vec!()
                };
            epiphone::node::Config::new(i, bind_addr, prev_node, 16, 64, solve) // 16 peers, 64 broadcast messages
        })
        .collect();

    let dbs: Vec<KvDb> = (0..num_nodes)
        .into_iter()
        .map(|_| {
            KvDb::new()
        })
        .collect();


    node_confs.iter().cloned()
        .enumerate()
        .map(|(i, c)| {
            let db = dbs[i].clone();
            let t = thread::spawn(move || {
                let db = KvService::new(i, db);
                let mut node = epiphone::Node::<usize, KvSolver>::new(c);
                info!("starting server");
                node.start(db);
                info!("server returned");
            });
            thread::sleep(Duration::from_millis(500)); // give time to spawn thread and handshake
            t
        }).count();


    let mut clients: Vec<Client<usize, KvSolver>> = node_confs.iter()
        .enumerate()
        .map(|(i, conf)| {
            let local = epiphone::node::LocalInfo::<usize>::new(num_nodes + i, null_addr);
            Client::connect(Config::publisher_from(local, conf.service.clone(), conf.local.addr))
                .and_then(|(c, _)| {
                    info!("test writer connected to {:?} @ {:?}", c.peer().unwrap(), c.peer_addr());
                    future::ok(c)
                })
                .wait()
                .expect("could not connect")
        })
        .collect();

    let make_key = || rand::thread_rng().gen_ascii_chars().take(key_len).collect::<String>();
    let make_val = || rand::thread_rng().gen_ascii_chars().take(val_len).collect::<String>();

    let writes: Vec<KvWrite> = (0..num_ops)
        .map(|_|{
            let key = make_key();
            let val = make_val();
            KvWrite::new(key, val, chrono::Utc::now())
        })
        .collect();
    info!("finished write generation");


    // start timing
    let start_time = time::precise_time_ns();

    let mut futs = writes.iter()
        .map(|w| {
            let msg = proto::Rumor::new(w.clone());
            let cli_idx = (rand::thread_rng().next_u32() as usize) % num_nodes;
            clients[cli_idx].send(msg)
        })
        .collect::<Vec<client::ResponseFuture>>();

    for fut in futs.drain(..) {
        match fut.wait().expect("broken response channel") {
            Err(e) => {
                info!("got error: {:?}", e);
                assert!(false, e);
            }
            Ok(rsp) => {
                rsp_count += 1;
                assert_eq!(rsp.hdr.msg_type, proto::MessageType::RumorResponse,
                    "expected Rumor response, got {:?}", rsp.hdr.msg_type);
                // TODO: deeper response verification?
            }
        }
    }


    let end_time = time::precise_time_ns();
    let write_diff = end_time - start_time; // ns
    let write_durr = time::Duration::nanoseconds(write_diff as i64);
    let writes_per_s = (num_ops * 1_000) / write_durr.num_milliseconds() as usize;
    info!("writes took {}.{}s, roughly {}/s", write_durr.num_seconds(), write_durr.num_milliseconds(), writes_per_s);

    thread::sleep(Duration::from_millis(15_000)); // give time to settle before locking the database

    // lock the databases
    let mut db_locks: Vec<MutexGuard<BTreeMap<KvKey, KvVal>>> = dbs.iter()
        .map(|d| d.lock().expect("poisoned db lock"))
        .collect();

    assert_eq!(rsp_count, num_ops);

    for (d, db) in db_locks.iter_mut().enumerate() {
        info!("node {:?} has {} values", d, db.len());
        assert_eq!(db.len(), num_ops, "db {:?} did not have {} keys", d, num_ops);
    }


    for (i, w) in writes.iter().enumerate() {
        for (d, db) in db_locks.iter_mut().enumerate() {
            assert_eq!(db.get(&w.key), Some(&w.val), "db {:?} did not have write {}: {}", d, i, w.key);
        }
    }
}



//------------------------------------------------
//
// helpers
//
//------------------------------------------------


#[macro_use]
extern crate log;
extern crate fern;
use std::sync::{Once, ONCE_INIT};
static LOGGING: Once = ONCE_INIT;

fn setup_logging() {
    LOGGING.call_once(|| {
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!("{}[{}][{}] {}",
                    chrono::Local::now()
                        .format("[%Y-%m-%d][%H:%M:%S]"),
                    record.target(),
                    record.level(),
                    message))
            })
            .level(log::LogLevelFilter::Info)
            //.level(log::LogLevelFilter::Debug)
            .chain(std::io::stdout())
            .apply().expect("could not init logging")
    })
}
