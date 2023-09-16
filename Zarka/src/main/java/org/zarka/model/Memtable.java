package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.CommitLog;
import org.zarka.avro.WeatherData;

import java.util.List;

public class Memtable {
    private RedBlackTree store;
    private CommitLog wal;
    private final Integer MEMTABLE_THRESHOLD = 512; // in KB
    private static Logger logger = LogManager.getLogger(Memtable.class);

    public Memtable() {
        store = new RedBlackTree();
        wal = new CommitLog("logs/commitLog");
        this.applyLog();
    }

    public void applyLog() {
        logger.info("Applying commit log");
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
        logger.info("Applied commit log: " + walEntries.size() + " entries");
    }

    private void applyEntries(List<WALEntry> walEntries) {
        // TODO: Apply entries to memtable
        for (WALEntry entry : walEntries) {
            store.insert(entry.getData());
            logger.info("Applying entry: " + entry.getData().toString());
        }
    }

    public void put(WeatherData data) {
        wal.appendLog(data);
        logger.info("Logging WeatherData to commit log");
        store.insert(data);
        // if memtable exceeds a certain size, flush to disk
        if (store.getSize() >= MEMTABLE_THRESHOLD) {
            // TODO: Flush to SSTABLE
            logger.info("Flushing memtable to disk");
        }
    }

    public void closeMemtable() {
        wal.closeCommitLog();
    }
}
