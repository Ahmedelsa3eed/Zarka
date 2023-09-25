package org.zarka;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;
import org.zarka.sstable.SSTableReader;
import org.zarka.sstable.SSTableWriter;

import java.io.IOException;

public class ZarkaNode {
    private Memtable memtable;
    private SSTableWriter ssTableWriter;
    private SSTableReader ssTableReader;
    private CommitLog commitLog; // separate commitLog for each memtable
    protected static final Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        ssTableWriter = new SSTableWriter();
        commitLog = new CommitLog();
        memtable = new Memtable(commitLog);
        memtable.applyLog();
        memtable.deleteRecoveredLog();
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

    /**
     * Check the memtable to see if the required data is present
     * 
     * @param id The id of the wheather station to read
     * @return The weather data for the given id
     */
    public WeatherData get(long id) {
        
        // check memtable first
        WeatherData data = memtable.get(id);
        if (data != null) {
            logger.info("Reading from memtable");
            return data;
        }

        // retrieve data from sstable
        if (ssTableReader == null) {
            ssTableReader = new SSTableReader();
        }
        logger.info("Reading from SSTable");
        data = ssTableReader.get(id);

        return data;
    }

    public void close() {
        ssTableWriter.shutdown();
        memtable.closeCommitLog();
    }

    protected void setMemtable(Memtable memtable) {
        this.memtable = memtable;
    }

    protected void setSSTableReader(SSTableReader ssTableReader) {
        this.ssTableReader = ssTableReader;
    }

    public static void main(String[] args) throws IOException {
        ZarkaNode node = new ZarkaNode();
        // Prepare test data
        long i;
        for (i = 1; i < 20; i++) {
            WeatherData data = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
            node.put(data);
        }
        WeatherData testData = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        node.put(testData);
        System.out.println("Read from memtable: " + node.get(i).toString());

        // Reading from sstable
        for (; i <= 40; i++) {
            WeatherData data = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
            node.put(data);
        }
        WeatherData res = node.get(20);
        System.out.println("Read from sstable: " + res.toString());

        WeatherData res2 = node.get(40);
        System.out.println("Read from memtable: " + res2.toString());

        node.close();
    }
}
