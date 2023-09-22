package org.zarka.model;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.CommitLog;
import org.zarka.avro.WeatherData;
import org.zarka.sstable.SSTableWriter;

import java.util.List;

public class Memtable {
    private RedBlackTree store;
    private CommitLog wal; // remove while removing memtable
    private final Integer MEMTABLE_THRESHOLD = 8; // # nodes in RB tree
    private static Logger logger = LogManager.getLogger(Memtable.class);

    public Memtable() {
        store = new RedBlackTree();
        wal = new CommitLog();
        applyLog();
    }

    public void applyLog() {
        logger.info("Applying commit log");
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
        logger.info("Applied commit log: " + walEntries.size() + " entries");
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry entry : walEntries) {
            logger.info("Applying entry with key: " + entry.getData().getStationId());
            store.insert(entry.getData());
        }
    }

    public void put(WeatherData data) {
        wal.appendLog(data);
        store.insert(data);
    }

    public boolean exceedsThreshold() {
        return store.getNodesCount() >= MEMTABLE_THRESHOLD;
    }

    public List<WeatherData> getInOrder() {
        return store.inorder();
    }

    public boolean clearCommitLog() {
        return wal.clear();
    }

}
