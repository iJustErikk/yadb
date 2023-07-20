use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::fs::File;
use tokio::time::{Duration, self};
use tokio::io::AsyncWriteExt;
use tokio::task;
use tokio::io::AsyncSeekExt;


pub struct AsyncBufferedWriter {
    channel: mpsc::Sender<WriterCommand>,
    flush_limit: usize,
}

enum WriterCommand {
    Write(Vec<u8>, oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

impl AsyncBufferedWriter {
    pub fn new(flush_limit: usize, time_limit: Duration, file: File) -> Self {
        // TODO: what is a good buffer length?
        // how to handle backpressure?
        let (tx, mut rx) = mpsc::channel(100000);

        let async_buffered_writer = AsyncBufferedWriter {
            channel: tx,
            flush_limit,
        };

        task::spawn(async move {
            let mut interval = time::interval(time_limit);
            let mut file = file;
            let mut to_notify: Vec<oneshot::Sender<()>> = Vec::new();
            let mut data_to_flush: Vec<u8> = Vec::new();
            

            loop {
                tokio::select! {
                    Some(command) = rx.recv() => match command {
                        // shutdown is called when we flush memtable and no longer need WAL
                        // we'll just set the cursor back to 0 so we can keep writing
                        WriterCommand::Shutdown(notifier) => {
                            // would only call shutdown when wal needs reset
                            // whoever calling this has the write lock on the memtable and wal
                            // any previous writes are now persisted via sstable, so no need to flush

                            // correctness considerations for wal:
                            // should be fine with channel reordering (new writes belonging to new wal), exhaustively:
                            // old write prior to shutdown: this is expected
                            // old write after shutdown: can this happen? this would duplicate state,
                            // this may push a value "back in time". 
                            // old write after shutdown can happen in this case:
                            // old write releases lock to allow for more concurrency
                            // new write (same key) goes all the way through (either persisted to wal or sst)
                            // old write finally is able to persist to WAL and thus the value is moved back in time
                            // but the old write never went through- so this is not moving back in time. they are concurrent. still has memory/read amplification
                            // these writes can be ignored via a global c
                            // new write prior to shutdown cant happen- any shutdown blocks future writes via write lock
                            // new write after shutdown - this is expected
                            // should be fine with dropping write lock 
                            // on memtable/wal when waiting for write to go through (to allow for better concurrency)
                            // does this result in any issues?
                            // us awaiting the WAL persist is the very last thing we do
                            // at any time, the lock could be sniped or there could be others awaiting for their write to go through
                            // others awaiting for their write to go through is fine
                            // if lock is sniped, then others may send a shutdown or write
                            // if a shutdown is sent -> value will be persisted in SST. this is the case where an old write happens after shutdown and the case where a value may be "pushed back in time"
                            // if a write is sent -> may trigger flush, but that is fine.
                            // does shutdown need to drop write lock (no it cant. it does not need to encourage any concurrency and it needs to ensure future writes do not go ahead of the shutdown- they might be lost)

                            file.write_all(&data_to_flush).await.unwrap();
                            file.sync_all().await.unwrap();

                            for notifier in to_notify.drain(..) {
                                notifier.send(()).unwrap();
                            }
                            file.seek(std::io::SeekFrom::Start(0)).await.unwrap();

                            // let shutdown requester know the wal_file cursor has been reset
                            notifier.send(()).unwrap();

                            data_to_flush.clear();
                            break;
                        }
                        WriterCommand::Write(data, notifier) => {
                            data_to_flush.extend(&data);
                            to_notify.push(notifier);

                            if data_to_flush.len() >= async_buffered_writer.flush_limit {
                                // TODO: would data_to_flush this copy paste into closure- but async closures are unstable?
                                file.write_all(&data).await.unwrap();
                                file.sync_all().await.unwrap();
                
                                for notifier in to_notify.drain(..) {
                                    notifier.send(()).unwrap();
                                }
                
                                data_to_flush.clear();
                            }
                        }
                    },
                    _ = interval.tick() => {
                        if !data_to_flush.is_empty() {
                            file.write_all(&data_to_flush).await.unwrap();
                            file.sync_all().await.unwrap();
            
                            for notifier in to_notify.drain(..) {
                                notifier.send(()).unwrap();
                            }
            
                            data_to_flush.clear();
                        }
                    },
                    
                }
            }
        });
        // return writer after spawning task
        async_buffered_writer
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.channel.send(WriterCommand::Write(data, tx)).await.unwrap();
        // NOTE: if writer task responds before or after we await, nothing is lost
        // so, no race condition.
        rx.await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.channel.send(WriterCommand::Shutdown(tx)).await.unwrap();
        rx.await?;
        Ok(())
    }
}
