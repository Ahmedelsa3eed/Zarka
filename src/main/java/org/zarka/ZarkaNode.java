package org.zarka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.model.Memtable;
import org.zarka.model.Pair;

public class ZarkaNode {
    private Memtable memtable;

    public ZarkaNode() {
        memtable = new Memtable();
    }

    public void put(Integer key, String value) {
        memtable.put(new Pair(key, value));
    }

}
