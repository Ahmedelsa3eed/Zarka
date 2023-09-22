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
    protected static final Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        ssTableWriter = new SSTableWriter();
        memtable = new Memtable();
    }

    public void put(WeatherData data) {
        logger.info("Writing data with id: " + data.getStationId());
        // update the memtable and flush if nessecary
        memtable.put(data);
        if (memtable.exceedsThreshold()) {
            logger.info("Flushing memtable to disk asynchronously");
            ssTableWriter.writeAsync(memtable);
            memtable = new Memtable();
        }
    }

    public void close() {
        ssTableWriter.shutdown();
        // commitLog.closeCommitLog();
    }

    public static void main(String[] args) throws IOException {
        ZarkaNode node = new ZarkaNode();
        WeatherData data1 = WeatherData.newBuilder()
                .setStationId(1)
                .setSNo(1)
                .setBatteryStatus("low")
                .setStatusTimestamp(1681521224)
                .setWeather(Weather.newBuilder()
                        .setHumidity(35)
                        .setTemperature(100)
                        .setWindSpeed(13)
                        .build())
                .build();
        WeatherData data2 = new WeatherData(2L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data3 = new WeatherData(3L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data4 = new WeatherData(4L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data5 = new WeatherData(5L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data6 = new WeatherData(6L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data7 = new WeatherData(7L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data8 = new WeatherData(8L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        node.put(data1);
        node.put(data2);
        node.put(data3);
        node.put(data4);
        node.put(data5);
        node.put(data6);
        node.put(data7);
        node.put(data8);
        // close to test the commitLog
        node.close();
    }
}
