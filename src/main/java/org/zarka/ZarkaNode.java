package org.zarka;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;
import org.zarka.sstable.SSTableWriter;

import java.io.IOException;

public class ZarkaNode {
    private Memtable memtable;
    private SSTableWriter ssTableWriter;
    private CommitLog commitLog; // separate commitLog for each memtable
    protected static final Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        ssTableWriter = new SSTableWriter();
        commitLog = new CommitLog();
        memtable = new Memtable(commitLog);
        memtable.applyLog();
    }

    public void put(WeatherData data) {
        logger.info("Writing data with id: " + data.getStationId());
        // update the memtable and flush if nessecary
        memtable.put(data);
        if (memtable.exceedsThreshold()) {
            logger.info("Flushing memtable to disk asynchronously");
            ssTableWriter.writeAsync(memtable);

            // Create a new commit log for the new memtable
            commitLog = new CommitLog();

            // Replace the old memtable with a new empty one
            memtable = new Memtable(commitLog);
        }
    }

    public WeatherData get(long id) {
        logger.info("Reading data with id: " + id);
        // check memtable first
        WeatherData data = memtable.get(id);
        if (data != null) {
            logger.info("Found data in memtable");
            return data;
        }
        // check sstables
        return null;
    }

    public void close() {
        ssTableWriter.shutdown();
        memtable.closeCommitLog();
    }

    public static void main(String[] args) throws IOException {
        ZarkaNode node = new ZarkaNode();
        for (long i = 1; i < 35; i++) {
            WeatherData data = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
            node.put(data);
        }
        node.close();
    }
}
