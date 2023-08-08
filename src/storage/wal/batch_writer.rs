use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot::{self, Receiver};
use tokio::fs::File;
use tokio::time::{Duration, self};
use tokio::io::AsyncWriteExt;
use tokio::task;
use tokio::io::{AsyncSeekExt, SeekFrom};



pub struct AsyncBufferedWriter {
    channel: UnboundedSender<WriterCommand>,
}

enum WriterCommand {
    Write(Vec<u8>, oneshot::Sender<()>),
    Reset(oneshot::Sender<()>),
}


// there is a library for this- tower-batch or smth like that
impl AsyncBufferedWriter {
    pub fn new(flush_limit: usize, time_limit: Duration, file: File) -> Self {
        // TODO: what is a good buffer length?
        // TODO: revisit this. buffer should not be unbounded (doing this to make )
        // TODO: revisit error handling (maybe pass it back up?)
        // how to handle backpressure?
        let (tx, mut rx) = unbounded_channel();

        let async_buffered_writer = AsyncBufferedWriter {
            channel: tx,
        };

        task::spawn(async move {
            let mut interval = time::interval(time_limit);
            let mut file = file;
            let mut to_notify: Vec<oneshot::Sender<()>> = Vec::new();
            let mut data_to_flush: Vec<u8> = Vec::new();
            

            loop {
                tokio::select! {
                    Some(command) = rx.recv() => match command {
                        // reset is called when we flush memtable and no longer need WAL
                        // we'll just set the cursor back to 0 so we can keep writing
                        WriterCommand::Reset(notifier) => {
                            // would only call reset when wal needs reset
                            // whoever calling this has the write lock on the memtable and wal
                            // any previous writes are now persisted via sstable, so no need to flush

                            // correctness considerations for wal:
                            // should be fine with channel reordering (new writes belonging to new wal), exhaustively:
                            // old write prior to reset: this is expected
                            // old write after reset: can this happen? this would duplicate state,
                            // this may push a value "back in time". 
                            // old write after reset can happen in this case:
                            // old write releases lock to allow for more concurrency
                            // new write (same key) goes all the way through (either persisted to wal or sst)
                            // old write finally is able to persist to WAL and thus the value is moved back in time
                            // but the old write never went through- so this is not moving back in time. they are concurrent. still has memory/read amplification
                            // these writes can be ignored via a global version
                            // new write prior to reset cant happen- any reset blocks future writes via write lock
                            // new write after reset - this is expected
                            // should be fine with dropping write lock 
                            // on memtable/wal when waiting for write to go through (to allow for better concurrency)
                            // does this result in any issues?
                            // us awaiting the WAL persist is the very last thing we do
                            // at any time, the lock could be sniped or there could be others awaiting for their write to go through
                            // others awaiting for their write to go through is fine
                            // if lock is sniped, then others may send a reset or write
                            // if a reset is sent -> value will be persisted in SST. this is the case where an old write happens after reset and the case where a value may be "pushed back in time"
                            // if a write is sent -> may trigger flush, but that is fine.
                            // does reset need to drop write lock (no it cant. it does not need to encourage any concurrency and it needs to ensure future writes do not go ahead of the reset- they might be lost)

                            file.write_all(&data_to_flush).await.unwrap();
                            file.sync_data().await.unwrap();

                            for notifier in to_notify.drain(..) {
                                notifier.send(()).unwrap();
                            }
                            file.seek(SeekFrom::Start(0)).await.unwrap();
                            // wal persisted, truncate now
                            file.set_len(0).await.unwrap();
                            file.sync_all().await.unwrap();

                            // let reset requester know the wal_file cursor has been reset
                            notifier.send(()).unwrap();

                            // println!("shutdown");
                            data_to_flush.clear();
                        }
                        WriterCommand::Write(data, notifier) => {
                            data_to_flush.extend(&data);
                            to_notify.push(notifier);

                            if data_to_flush.len() >= flush_limit {
                                // TODO: would data_to_flush this copy paste into closure- but async closures are unstable?
                                file.write_all(&data_to_flush).await.unwrap();
                                file.sync_data().await.unwrap();
                
                                for notifier in to_notify.drain(..) {
                                    notifier.send(()).unwrap();
                                }
                
                                data_to_flush.clear();
                                // println!("acc flush");
                                
                            }
                        }
                    },
                    _ = interval.tick() => {
                        if !data_to_flush.is_empty() {
                            file.write_all(&data_to_flush).await.unwrap();
                            file.sync_data().await.unwrap();
                            
            
                            for notifier in to_notify.drain(..) {
                                notifier.send(()).unwrap();
                            }
            
                            data_to_flush.clear();
                            // println!("interval flush");
                        }
                    },
                    
                }
            }
        });
        // return writer after spawning task
        async_buffered_writer
    }

    pub fn write(&self, data: Vec<u8>) -> Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.channel.send(WriterCommand::Write(data, tx)).unwrap();
        
        // NOTE: if writer task responds before or after we await, nothing is lost
        // so, no race condition.
        return rx;
    }

    pub async fn reset(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.channel.send(WriterCommand::Reset(tx)).unwrap();
        rx.await?;
        Ok(())
    }
}
