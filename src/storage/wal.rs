
extern crate byteorder;
use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::Write;
use std::io::Read;
use std::convert::TryFrom;
pub struct WALEntry {
    // see below, these should be hidden
    pub operation: Operation,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WALEntry {
    // TODO: wal buffer means this can become private
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self.operation as u8)?;
        writer.write_u64::<LittleEndian>(self.key.len() as u64)?;
        writer.write_all(&self.key)?;
        writer.write_u64::<LittleEndian>(self.value.len() as u64)?;
        writer.write_all(&self.value)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    // this will fail if entry becomes corrupted or the write failed midway
    // for either, let's make the assumption that it happened on the last walentry
    // read to the end of the file
    // comeback and think of a better solution
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let operation = reader.read_u8()?;
        let operation = Operation::try_from(operation).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid operation"))?;
        let key_len = reader.read_u64::<LittleEndian>()? as usize;
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;
        let value_len = reader.read_u64::<LittleEndian>()? as usize;
        let mut value = vec![0; value_len];
        reader.read_exact(&mut value)?;
        let mut newline = [0; 1];
        reader.read_exact(&mut newline)?;

        
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
#[derive(PartialEq)]
#[derive(Copy, Clone)]
pub enum Operation {
    GET = 0, PUT = 1, DELETE = 2
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