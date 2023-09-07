use std::collections::HashSet;
use std::error::Error;
use std::{fmt, io};

// TODO: add more errors
// I'm wrapping IOError for now, but this is not useful for the end user. Probably should just return some unrecoverable error and tell them to restart process/server.
// the user is going to want specific, actionable errors
#[derive(Debug)]
pub enum YAStorageError {
    // TODO: should just panic if there is an ioerror, as it cannot help the user
    IOError { error: io::Error },
    MissingHeader,
    CorruptedHeader,
    UnexpectedUserFolder { folderpath: String },
    UnexpectedUserFile { filepath: String },
    LevelTooLarge { level: u8 },
    FSInitMismatch { expected: HashSet<String>, actual: HashSet<String> },
    CorruptWalEntry,
    MissingWAL,
}

impl YAStorageError {
    fn new(error: io::Error) -> YAStorageError {
        YAStorageError::IOError { error }
    }
}

impl From<io::Error> for YAStorageError {
    fn from(error: io::Error) -> Self {
        YAStorageError::new(error)
    }
}

impl fmt::Display for YAStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            YAStorageError::MissingHeader => write!(f, "Missing header"),
            YAStorageError::CorruptedHeader => write!(f, "Corrupted header"),
            YAStorageError::UnexpectedUserFolder { folderpath } => {
                write!(f, "Unexpected user folder: {}", folderpath)
            }
            YAStorageError::UnexpectedUserFile { filepath } => {
                write!(f, "Unexpected user file: {}", filepath)
            }
            YAStorageError::LevelTooLarge { level } => write!(f, "Level too large: {}", level),
            YAStorageError::FSInitMismatch { expected, actual } => write!(
                f,
                "SSTable mismatch: expected {:?}, got {:?}",
                expected, actual
            ),
            YAStorageError::CorruptWalEntry => write!(f, "Corrupt Wal Entry"),
            YAStorageError::IOError { error } => write!(f, "IO Error: {}", error.to_string()),
            YAStorageError::MissingWAL => write!(f, "Missing WAL"),
        }
    }
}

impl Error for YAStorageError {}
