package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.CommitLog;

import java.util.List;

public class Memtable {
    private RedBlackTree store;
    private CommitLog wal;
    private final Integer MEMTABLE_THRESHOLD = 512; // in KB
    private static Logger logger = LogManager.getLogger(Memtable.class);

    public Memtable() {
        store = new RedBlackTree();
        wal = new CommitLog("commitLog.log");
        this.applyLog();
    }

    public void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        // TODO: Apply entries to memtable
    }

    public void put(Pair pair) {
        store.insert(pair);
        // if memtable exceeds a certain size, flush to disk
        if (store.getSize() >= MEMTABLE_THRESHOLD) {
            // TODO: Flush to SSTABLE
            logger.info("Flushing memtable to disk");
        }
    }
}
