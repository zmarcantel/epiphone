extern crate bytes;
extern crate futures;
use futures::{BoxFuture};

extern crate tokio_io;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

extern crate serde;
use serde::{Serialize, Deserialize};
#[macro_use]
extern crate serde_derive;

extern crate bincode;

#[macro_use]
extern crate log;

#[macro_use]
pub mod proto;

pub mod node;
pub use node::Node;

pub mod client;
pub use client::Client;

pub mod peer;

pub mod error;
pub use error::Error;


use std::fmt;

pub trait UniqueProvider {
    type Unique: Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de>;
    fn get_unique(&self) -> &Self::Unique;
}

pub trait App: Clone + Send + Sync + proto::challenge::Challenger {
    type Rumor: fmt::Debug + UniqueProvider + Serialize + for<'de> Deserialize<'de>;
    type Challenge;
    type ChallengeResponse;

    fn has_unique(&self, uniq: &<Self::Rumor as UniqueProvider>::Unique) -> BoxFuture<bool, Error>;
    fn handle(&self, rumor: Self::Rumor) -> BoxFuture<proto::RumorResponse, Error>;
}
