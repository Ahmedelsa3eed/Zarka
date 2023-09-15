package org.zarka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.model.Memtable;
import org.zarka.model.Pair;

public class ZarkaNode {
    private Memtable memtable;
    protected static final Logger logger = LogManager.getLogger();

    public ZarkaNode() {
        memtable = new Memtable();
    }

    public void put(Integer key, String value) {
        logger.info("Putting key: " + key);
        memtable.put(new Pair(key, value));
    }

    public void close() {
        memtable.closeMemtable();
    }

    public static void main(String[] args) {
        String test = """
        {
            "station_id": 1, // Long
            "s_no": 1, // Long auto-incremental with each message per service
            "battery_status": "low", // String of (low, medium, high)
            "status_timestamp": 1681521224, // Long Unix timestamp
            "weather": {
                "humidity": 35, // Integer percentage
                "temperature": 100, // Integer in fahrenheit
                "wind_speed": 13, // Integer km/h
            }
        }""";
        ZarkaNode node = new ZarkaNode();
        node.put(1, test);
        node.put(2, test);
        node.put(3, test);
        node.put(4, test);
        node.put(5, test);
        node.put(6, test);
        node.put(7, test);
        node.put(8, test);
        node.put(9, test);
        node.put(10, test);
        node.close();
    }
}
