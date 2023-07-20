use crossbeam_skiplist::SkipMap;

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::fs::File;
use tokio::time::Duration;
use std::convert::TryFrom;

mod batch_writer;
use batch_writer::AsyncBufferedWriter;
pub struct WALEntry {
    // see below, these should be hidden
    pub operation: Operation,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WALEntry {
    // TODO: wal buffer means this can become private
    pub async fn serialize<W: io::AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self.operation as u8).await?;
        writer.write_u64_le(self.key.len() as u64).await?;
        writer.write_all(&self.key).await?;
        writer.write_u64_le(self.value.len() as u64).await?;
        writer.write_all(&self.value).await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }

    // this will fail if entry becomes corrupted or the write failed midway
    // for either, let's make the assumption that it happened on the last walentry
    // read to the end of the file
    // comeback and think of a better solution
    pub async fn deserialize<R: io::AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let operation = reader.read_u8().await?;
        let operation = Operation::try_from(operation)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid operation"))?;
        let key_len = reader.read_u64_le().await? as usize;
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key).await?;
        let value_len = reader.read_u64_le().await? as usize;
        let mut value = vec![0; value_len];
        reader.read_exact(&mut value).await?;
        let mut newline = [0; 1];
        reader.read_exact(&mut newline).await?;

        // if newline[0] != b'\n' {
        //     return Err(YAStorageError::CorruptWalEntry)?;
        // }

        Ok(WALEntry {
            operation,
            key,
            value,
        })
    }
}

#[derive(Debug)]
// could provide more ops in future, like the merge operator in pebble/rocksdb
#[derive(PartialEq, Copy, Clone)]
pub enum Operation {
    GET = 0,
    PUT = 1,
    DELETE = 2,
}

impl TryFrom<u8> for Operation {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::GET),
            1 => Ok(Operation::PUT),
            2 => Ok(Operation::DELETE),
            _ => Err(()),
        }
    }
}

pub struct WALFile {
    pub wal_file: Option<File>
}
const MAX_BYTES: u64 = 4096;
const TIME_LIMIT: Duration = Duration::from_millis(10);

impl WALFile {
    pub fn new() -> Self {
        return WALFile { wal_file: None }
    }

    pub async fn append_to_wal(&mut self, entry: WALEntry) -> io::Result<()> {
        entry
            .serialize(&mut (self.wal_file.as_mut().unwrap()))
            .await?;
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.as_mut().unwrap().sync_data().await?;
        Ok(())
    }

    pub async fn reset(&mut self) -> io::Result<()> {
            // wal persisted, truncate now
            self.wal_file.as_mut().unwrap().set_len(0).await?;
            self.wal_file.as_mut().unwrap().sync_all().await?;
            Ok(())
    }

    pub async fn get_wal_as_skipmap(&mut self) -> io::Result<SkipMap<Vec<u8>, Vec<u8>>> {
        let mut entries = Vec::new();
        loop {
            match WALEntry::deserialize(&mut (self.wal_file.as_mut().unwrap())).await {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        let skipmap = SkipMap::new();

        for entry in entries {
            match entry.operation {
                Operation::PUT => {
                    skipmap.insert(entry.key, entry.value);
                }
                // empty vector is tombstone
                Operation::DELETE => {
                    skipmap.insert(entry.key, Vec::new());
                }
                _ => {}
            }
        }
        return Ok(skipmap);
    }
}