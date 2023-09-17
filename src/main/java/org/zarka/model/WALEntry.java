package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.WeatherData;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WALEntry {
    private final long entryIndex;
    private final WeatherData data;
    private final long timeStamp;
    private static Logger logger = LogManager.getLogger(WALEntry.class);

    public WALEntry(Long entryIndex, WeatherData data, long timeStamp) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.timeStamp = timeStamp;
    }

    public WeatherData getData() {
        return data;
    }

    /**
     * format: entryIndex: long | timeStamp: long | k-vPair-len: int | k-vPair: byte[]
     * */
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeLong(entryIndex);
        dos.writeLong(timeStamp);
        byte[] serializedData = data.toByteBuffer().array();
        dos.writeInt(serializedData.length);
        dos.write(serializedData);
    }

    /**
     * Deserialize all entries from commit log
     * */
    public static List<WALEntry> deserialize(InputStream is) {
        List<WALEntry> entries = new ArrayList<>();
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            while (dataInputStream.available() > 0) {
                long entryIndex = dataInputStream.readLong();
                long timeStamp = dataInputStream.readLong();
                int dataLength = dataInputStream.readInt();
                WeatherData deserializedData = WeatherData.fromByteBuffer(ByteBuffer.wrap(dataInputStream.readNBytes(dataLength)));
                entries.add(new WALEntry(entryIndex, deserializedData, timeStamp));
                logger.info("entryIndex: " + entryIndex + " timeStamp: " + timeStamp + " keyValueLength: " + deserializedData.toString());
            }
        } catch (IOException e) {
            logger.error("Error deserializing WALEntry", e);
        }
        return entries;
    }
}
