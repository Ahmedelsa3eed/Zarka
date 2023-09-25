package org.zarka.sstable;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.WeatherData;

/**
 * Read data from SSTable files involves reading the data file, deserializing
 * the stored data, and efficiently finding the data associated with a specific
 * key
 */
public class SSTableReader {
    private File ssTablesDir; // Path to SSTable files
    private Map<Long, WeatherData> dataMap; // In-memory cache for faster lookups
    private static Logger logger = LogManager.getLogger(SSTableReader.class);

    public SSTableReader() {
        this.ssTablesDir = new File("data/");
        this.dataMap = new HashMap<>();
        loadSSTables();
    }

    /**
     * Load all SSTable files into memory
     */
    private void loadSSTables() {
        File[] ssTableFiles = ssTablesDir.listFiles();
        if (ssTableFiles != null && ssTableFiles.length > 0) {
            Arrays.sort(ssTableFiles, Comparator.comparing(File::getName));

            for (File sstable : ssTableFiles) {
                try (FileInputStream fis = new FileInputStream(sstable);
                    DataInputStream dis = new DataInputStream(fis)) {
    
                        while (dis.available() > 0) {
                            int dataLength = dis.readInt();
                            byte[] data = dis.readNBytes(dataLength);
                            WeatherData weatherData = WeatherData.fromByteBuffer(ByteBuffer.wrap(data));
                            dataMap.put(weatherData.getStationId(), weatherData);
                        }
                } catch (IOException e) {
                    logger.error("Error reading SSTable file: " + sstable.getName(), e);
                }
            }
        }
    }

    public WeatherData get(long id) {
        return dataMap.get(id);
    }
}
