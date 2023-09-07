# FS Layout

## What is desired?

The storage engine should be **durable**. That is, it should gracefully tolerate the OS or even a user shutting down the process. As long as the database is not corrupted, it should be able to recover (and not lose writes). This should be the case whenever, even if the database is in the middle of compaction or memtable flush. 

## What Needs to be Addressed?

Currently, there are 2 points when the FS changes (files created/deleted): during memtable flush (write new table) and compaction (reading 10 tables, writing a new table, a potentially deferred delete of those 10 tables). The current implementation will panic when it learns that the failed table is unexpected wrt the header after a failed memtable flush. A failed compaction could have the same result. This could also happen with a WAL restoration.

Currently, the FS layout does have enough flexibility for keeping around stale tables so that table iterators can stay valid. 

## Solution

The header file format will change. It will need to store a mapping of file hashname to file metadata (level, name, whatever else).

When updating: Simply just replace the header. Any tables not serving any purpose on startup are deleted (failed memtable/wal flushes, failed compaction). WAL restore/compaction are retried. 

To guarantee this, only reset WAL/delete old level after header has been successfully committed.


Note: there will not be issues with flushing header. UNIX writes are atomic (for single block writes). So, the header will either only be the new one or the old one. If the write failed, then we will have to redo the operation, but that is much better than losing the database.