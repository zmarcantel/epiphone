use bincode;
use futures;

use proto::{MessageType};

use std::{io, fmt, error};
use std::error::Error as StdError;

#[derive(Debug)]
pub enum ErrorKind {
    Deframe,
    SolveError,
    SolveFail,
    UnexpectedType(Vec<MessageType>, MessageType),
}
impl ErrorKind {
    pub fn as_str(&self) -> &str {
        match *self {
            ErrorKind::Deframe => { "failed to deframe message" }
            ErrorKind::SolveError => { "error while solving challenge" }
            ErrorKind::SolveFail => { "failed to solve challenge" }
            ErrorKind::UnexpectedType(_, _) => { "unexpected message type" }
        }
    }
}
impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            ErrorKind::UnexpectedType(ref exp, ref got) => {
                write!(f, "expected one of {:?}, got {:?}", exp, got)
            }
            _ => { write!(f, "{}", self.as_str()) }
        }
    }
}


// TODO: IntoFuture trait
#[derive(Debug)]
pub enum Error {
    IO(Option<String>, io::Error),
    Serde(Option<String>, Box<bincode::ErrorKind>),
    Proto(ErrorKind, String),
    Generic(String),
}
impl Error {
    pub fn new(err: String) -> Error {
        Error::Generic(err)
    }

    pub fn io(desc: Option<String>, err: io::Error) -> Error {
        Error::IO(desc, err)
    }
    pub fn io_other(err: String) -> Error {
        Error::IO(None, io::Error::new(io::ErrorKind::Other, err))
    }

    pub fn serde(desc: Option<String>, err: Box<bincode::ErrorKind>) -> Error {
        Error::Serde(desc, err)
    }

    //
    // proto error kinds
    //

    pub fn proto(kind: ErrorKind, err: String) -> Error {
        Error::Proto(kind, err)
    }
    pub fn deframe(err: String) -> Error {
        Error::Proto(ErrorKind::Deframe, err)
    }
    pub fn solve_err(err: String) -> Error {
        Error::Proto(ErrorKind::SolveError, err)
    }
    pub fn solve_fail(err: String) -> Error {
        Error::Proto(ErrorKind::SolveFail, err)
    }
    pub fn unexpected(allow: Vec<MessageType>, got: MessageType, err: String) -> Error {
        Error::Proto(ErrorKind::UnexpectedType(allow, got), err)
    }
}
impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref d, ref e) => {
                if let Some(ref desc) = *d {
                    desc.as_str()
                } else {
                    e.description()
                }
            }
            Error::Serde(ref d, ref e) => {
                if let Some(ref desc) = *d {
                    desc.as_str()
                } else {
                    e.description()
                }
            }
            Error::Proto(ref d, _) => {
                d.as_str()
            }
            Error::Generic(ref e) => { e.as_str() }
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::IO(_, ref e) => { Some(e) }
            Error::Serde(_, ref e) => { Some(e) }

            Error::Proto(_, _) | Error::Generic(_) => { None }
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::IO(ref d, ref e) => {
                if d.is_some() {
                    write!(f, "{}: {}", d.as_ref().unwrap(), e.description())
                } else {
                    write!(f, "{}", e.description())
                }
            }
            Error::Serde(ref d, ref e) => {
                if d.is_some() {
                    write!(f, "{}: {}", d.as_ref().unwrap(), e.description())
                } else {
                    write!(f, "{}", e.description())
                }
            }
            Error::Proto(ref d, ref e) => {
                match *d {
                    ErrorKind::UnexpectedType(_, _) => {
                        write!(f, "{}: {}", e, d)
                    }
                    _ => {
                        write!(f, "{}: {}", d, e)
                    }
                }
            }
            Error::Generic(ref e) => {
                write!(f, "{}", e.as_str())
            }
        }
    }
}
impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::new(err)
    }
}
impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::io(None, err)
    }
}
impl From<futures::Canceled> for Error {
    fn from(_: futures::Canceled) -> Error {
        Error::io(None, io::Error::new(io::ErrorKind::Other, "future was canceled"))
    }
}
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Error {
        Error::serde(None, err)
    }
}
impl Into<io::Error> for Error {
    fn into(self) -> io::Error {
        match self {
            Error::IO(_, e) => {
                e
            }

            _ => {
                io::Error::new(io::ErrorKind::Other, self)
            }
        }
    }
}
