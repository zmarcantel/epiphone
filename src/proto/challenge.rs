use serde::{Serialize, Deserialize};

use std::fmt;

pub trait Challenger {
    type Challenge: Serialize + for<'de> Deserialize<'de> + fmt::Debug;
    type Response: Serialize + for<'de> Deserialize<'de> + fmt::Debug;

    fn generate(&self) -> Option<Self::Challenge>;
    fn check(&self, Self::Response) -> bool;
}

pub trait Solver {
    type Challenge: Serialize + for<'de> Deserialize<'de> + fmt::Debug;
    type Response: Serialize + for<'de> Deserialize<'de> + fmt::Debug;
    type Error: Serialize + for<'de> Deserialize<'de> + fmt::Debug;

    fn solve(&self, c: Self::Challenge) -> Result<Self::Response, Self::Error>; // TODO: non-io error
}

pub struct ErrorSolver;
impl Solver for ErrorSolver {
    type Challenge = ();
    type Response = ();
    type Error = String;

    fn solve(&self, _:Self::Challenge) -> Result<Self::Response, Self::Error> {
        Err("handshake challenge not expected".to_string())
    }
}
