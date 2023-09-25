package org.zarka;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.zarka.avro.Weather;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;
import org.zarka.sstable.SSTableReader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;

class ZarkaNodeTest {
    private ZarkaNode zarkaNode;
    private Memtable memtable;
    private SSTableReader ssTableReader;

    @BeforeEach
    public void setUp() {
        zarkaNode = new ZarkaNode();
        memtable = Mockito.mock(Memtable.class);
        ssTableReader = Mockito.mock(SSTableReader.class);
        zarkaNode.setMemtable(memtable);
        zarkaNode.setSSTableReader(ssTableReader);
    }

    @Test
    void whenCallingGet_itShouldReadFromMemtable() {
        // Prepare test data
        long i;
        for (i = 1; i < 20; i++) {
            WeatherData data = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
            zarkaNode.put(data);
        }
        WeatherData testData = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        zarkaNode.put(testData);

        // Configure memtable to return data for the given key
        when(memtable.get(i)).thenReturn(testData);

        // Perform the read operation
        WeatherData result = zarkaNode.get(i);

        // Assert that the data is retrieved from the memtable
        assertEquals(testData, result);
    }

    @Test
    void whenCallingGet_itShouldReadFromSSTable() {
        // Prepare test data
        long i;
        for (i = 1; i < 32; i++) {
            WeatherData data = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
            zarkaNode.put(data);
        }
        WeatherData testData = new WeatherData(i, 1L, "low", 1681521224L, new Weather(35, 100, 13));
        zarkaNode.put(testData);

        // Configure SSTableReader to return data for the given key
        when(memtable.get(i)).thenReturn(null);
        when(ssTableReader.get(i)).thenReturn(testData);

        // Perform the read operation
        WeatherData result = zarkaNode.get(i);

        // Assert that the data is retrieved from the memtable
        assertEquals(testData, result);
    }

}