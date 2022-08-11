use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub message: String,
}
impl std::error::Error for Error {}
impl Error {
    pub fn new(message: &str) -> Error {
        Error {
            message: message.to_string(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Self {
            message: format!("CSV Error: {}", err.to_string()),
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            message: format!("IO Error: {}", err.to_string()),
        }
    }
}
