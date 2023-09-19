package org.zarka;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;

import java.io.IOException;

public class ZarkaNode {
    private Memtable memtable;
    protected static final Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        memtable = new Memtable();
    }

    public void put(WeatherData data) {
        logger.info("Putting WeatherData with id: " + data.getStationId());
        // update the memtable and flush if nessecary
        memtable.put(data);
    }

    public void close() {
        memtable.closeMemtable();
    }

    public static void main(String[] args) throws IOException {
        ZarkaNode node = new ZarkaNode();
        WeatherData data1 = WeatherData.newBuilder()
                .setStationId(11)
                .setSNo(1)
                .setBatteryStatus("low")
                .setStatusTimestamp(1681521224)
                .setWeather(Weather.newBuilder()
                        .setHumidity(35)
                        .setTemperature(100)
                        .setWindSpeed(13)
                        .build())
                .build();
        WeatherData data2 = new WeatherData(12L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data3 = new WeatherData(13L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data4 = new WeatherData(14L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data5 = new WeatherData(15L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data6 = new WeatherData(16L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data7 = new WeatherData(17L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data8 = new WeatherData(18L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
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
