package org.zarka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.model.Pair;
import org.zarka.model.RedBlackTree;

public class ZarkaNode {
    private RedBlackTree memtable;
    private final Integer MEMTABLE_THRESHOLD = 512; // in KB
    private static Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        memtable = new RedBlackTree();
    }

    public void put(Integer key, String value) {
        memtable.insert(new Pair(key, value));
        // if memtable exceeds a certain size, flush to disk
        if (memtable.getSize() >= MEMTABLE_THRESHOLD) {
            // TODO: Flush to SSTABLE
        }
    }
}
