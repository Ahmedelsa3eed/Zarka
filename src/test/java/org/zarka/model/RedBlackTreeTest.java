package org.zarka.model;

import org.junit.jupiter.api.BeforeEach;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedBlackTreeTest {
    RedBlackTree bst;

    @BeforeEach
    void setUp() {
        bst = new RedBlackTree();
    }

    @org.junit.jupiter.api.Test
    void testInsertOne() {
        WeatherData data = WeatherData.newBuilder()
                .setStationId(1L)
                .setSNo(1L)
                .setBatteryStatus("low")
                .setStatusTimestamp(1681521224L)
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
        bst.insert(data);
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        List<WeatherData> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getStationId() <= res.get(i + 1).getStationId());
        }
        assertEquals(bst.getNodesCount(), 6);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeOne() {
        WeatherData data2 = new WeatherData(2L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data3 = new WeatherData(3L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data4 = new WeatherData(4L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data5 = new WeatherData(5L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data6 = new WeatherData(6L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        bst.deleteNode(4);
        List<WeatherData> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getStationId() <= res.get(i + 1).getStationId());
        }
        assertEquals(bst.getNodesCount(), 4);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeTwo() {
        WeatherData data2 = new WeatherData(2L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data3 = new WeatherData(3L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data4 = new WeatherData(4L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data5 = new WeatherData(5L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        WeatherData data6 = new WeatherData(6L, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        bst.deleteNode(3);
        bst.deleteNode(4);
        bst.deleteNode(5);
        List<WeatherData> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getStationId() <= res.get(i + 1).getStationId());
        }
        assertEquals(bst.getNodesCount(), 2);
    }
}