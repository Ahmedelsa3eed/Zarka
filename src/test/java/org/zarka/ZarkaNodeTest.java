package org.zarka;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.zarka.avro.Data;
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
            Data data = new Data(String.valueOf(i), "low");
            zarkaNode.put(data.getKey().toString(), data.getValue().toString());
        }
        Data testData = new Data(String.valueOf(i), "low");
        String testValue = testData.getValue().toString();
        zarkaNode.put(testData.getKey().toString(), testValue);

        // Configure memtable to return data for the given key
        when(memtable.get(String.valueOf(i))).thenReturn(testValue);

        // Perform the read operation
        String result = zarkaNode.get(String.valueOf(i));

        // Assert that the data is retrieved from the memtable
        assertEquals(testValue, result);
    }

    @Test
    void whenCallingGet_itShouldReadFromSSTable() {
        // Prepare test data
        long i;
        for (i = 1; i < 32; i++) {
            Data data = new Data(String.valueOf(i), "low");
            zarkaNode.put(data.getKey().toString(), data.getValue().toString());
        }
        Data testData = new Data(String.valueOf(i), "low");
        String testValue = testData.getValue().toString();
        zarkaNode.put(testData.getKey().toString(), testValue);

        // Configure SSTableReader to return data for the given key
        when(memtable.get(String.valueOf(i))).thenReturn(null);
        when(ssTableReader.get(String.valueOf(i))).thenReturn(testValue);

        // Perform the read operation
        String result = zarkaNode.get(String.valueOf(i));

        // Assert that the data is retrieved from the memtable
        assertEquals(testValue, result);
    }

}