package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.CommitLog;
import org.zarka.avro.Data;

import java.util.List;

public class Memtable {
    private RedBlackTree store;
    private CommitLog wal; // remove while removing memtable
    private final Integer MEMTABLE_THRESHOLD = 32; // # nodes in RB tree
    private static Logger logger = LogManager.getLogger(Memtable.class);

    public Memtable(CommitLog wal) {
        this.store = new RedBlackTree();
        this.wal = wal;
    }

    public void applyLog() {
        logger.info("Recovering commit log");
        List<WALEntry> walEntries = wal.recover();
        applyEntries(walEntries);
        logger.info("Applied commit log: " + walEntries.size() + " entries");
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry entry : walEntries) {
            logger.info("Applying entry with key: " + entry.getData().getKey());
            store.insert(entry.getData());
        }
    }

    public void deleteRecoveredLog() {
        wal.deleteRecoveredLog();
    }

    public void put(Data data) {
        wal.appendLog(data);
        store.insert(data);
    }

    public String get(String key) {
        CharSequence value = store.searchTree(key).getValue();
        return value != null ? value.toString() : null;
    }

    public boolean exceedsThreshold() {
        return store.getNodesCount() >= MEMTABLE_THRESHOLD;
    }

    public List<Data> getInOrder() {
        return store.inorder();
    }

    public void closeCommitLog() {
        wal.close();
    }

    public boolean clearCommitLog() {
        return wal.clear();
    }

}
