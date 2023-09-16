package org.zarka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;
import org.zarka.model.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ZarkaNode {
    private Memtable memtable;
    protected static final Logger logger = LogManager.getLogger();

    public ZarkaNode() {
        memtable = new Memtable();
    }

    public void put(WeatherData data) {
        logger.info("Putting WeatherData with id: " + data.getStationId());
        memtable.put(data);
    }

    public void close() {
        memtable.closeMemtable();
    }

    public static void main(String[] args) throws IOException {
       String test = """
       {
           "station_id": 1,
           "s_no": 1,
           "battery_status": "low",
           "status_timestamp": 1681521224,
           "weather": {
               "humidity": 35,
               "temperature": 100,
               "wind_speed": 13
           }
       }""";
       System.out.println("String byte lenght: " + test.getBytes().length);
//        ZarkaNode node = new ZarkaNode();
//        node.put(1, test);
//        node.put(2, test);
//        node.put(3, test);
//        node.put(4, test);
//        node.put(5, test);
//        node.put(6, test);
//        node.put(7, test);
//        node.put(8, test);
//        node.put(9, test);
//        node.put(10, test);
//        node.close();

        WeatherData data = WeatherData.newBuilder()
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
        byte[] bytes = data.toByteBuffer().array();
        System.out.println("Bytes: " + bytes.length);
        WeatherData deserialized = WeatherData.fromByteBuffer(ByteBuffer.wrap(bytes));
        System.out.println(deserialized.toString());
    }
}
