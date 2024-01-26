use core::fmt;

pub type Result<T> = std::result::Result<T, Error>;


#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Abort,
    Internal(String),
    Value(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
       match self {
           Error::Abort => write!(f, "Operation aborted"),
           Error::Value(message) | Error::Internal(message) => write!(f, "{}", message),
       } 
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Internal(value.to_string())
    }
}