package org.zarka.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zarka.CommitLog;
import org.zarka.avro.WeatherData;
import org.zarka.sstable.SSTable;

import java.util.List;

public class Memtable {
    private RedBlackTree store;
    private CommitLog wal;
    private SSTable sstable;
    private final Integer MEMTABLE_THRESHOLD = 8; // Number of nodes in RB tree
    private static Logger logger = LoggerFactory.getLogger(Memtable.class);

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
        logger.info("Logging WeatherData to the CommitLog");
        store.insert(data);
        // if memtable exceeds MEMTABLE_THRESHOLD, flush to disk
        if (store.getNodesCount() >= MEMTABLE_THRESHOLD) {
            logger.info("Flushing memtable to disk");
            sstable = new SSTable();
            sstable.write(this);
        }
    }

    public List<WeatherData> getInOrder() {
        return store.inorder();
    }

    public void closeMemtable() {
        wal.closeCommitLog();
    }
}
