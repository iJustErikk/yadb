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

    // pub fn serialize(&self) -> Vec<u8> {
    //     let mut buffer = Vec::new();
    
    //     buffer.push(self.operation as u8);
    //     buffer.extend_from_slice(&(self.key.len() as u64).to_le_bytes());
    //     buffer.extend(&self.key);
    //     buffer.extend_from_slice(&(self.value.len() as u64).to_le_bytes());
    //     buffer.extend(&self.value);
    //     buffer.push(b'\n');
    
    //     buffer
    // }

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
    pub wal_file: Option<File>,
    pub writer: Option<AsyncBufferedWriter>
}
const MAX_BYTES: usize = 4096;
const TIME_LIMIT: Duration = Duration::from_millis(10);

impl WALFile {
    pub fn new() -> Self {
        return WALFile { wal_file: None, writer: None }
    }

    pub fn start_writing(&mut self, file: File) {
        self.wal_file = Some(file);
    }

    pub async fn append_to_wal(&mut self, entry: WALEntry) -> io::Result<()> {
        // self.writer.as_mut().unwrap().write(entry.serialize())
        entry
            .serialize(&mut (self.wal_file.as_mut().unwrap()))
            .await?;
        // wal metadata should not change, so sync_data is fine to use, instead of sync_all/fsync
        self.wal_file.as_mut().unwrap().sync_data().await?;
        Ok(())
    }
    // this is temporary ugliness- this will be gone soon
    pub async fn reset(&self, wal_file: &mut Option<&mut File>) -> io::Result<()> {
            if wal_file.is_none() {
                return Ok(());
            }
            // wal persisted, truncate now
            let wal_file = &mut **wal_file.as_mut().unwrap();
            wal_file.set_len(0).await?;
            wal_file.sync_all().await?;
            Ok(())
    }

    pub async fn get_wal_as_skipmap(&self, wal_file: &mut File) -> io::Result<SkipMap<Vec<u8>, Vec<u8>>> {
        // whatever file this uses should get consumed, so it should get passed in
        // caller should call initialization function that resets for next run
        // that should also setup the batch writer
        let mut entries = Vec::new();
        loop {
            match WALEntry::deserialize(wal_file).await {
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