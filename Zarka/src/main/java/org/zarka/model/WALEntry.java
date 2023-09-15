package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WALEntry {
    private final long entryIndex;
    private final byte[] keyValuePair;
    private final long timeStamp;
    private static Logger logger = LogManager.getLogger(WALEntry.class);

    public WALEntry(Long entryIndex, byte[] keyValuePair, long timeStamp) {
        this.entryIndex = entryIndex;
        this.keyValuePair = keyValuePair;
        this.timeStamp = timeStamp;
    }

    public byte[] getKeyValuePair() {
        return keyValuePair;
    }

    /**
     * format: entryIndex: long | timeStamp: long | k-vPair-len: int | k-vPair: byte[]
     * */
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeLong(entryIndex);
        dos.writeLong(timeStamp);
        dos.writeInt(keyValuePair.length);
        dos.write(keyValuePair);
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
                int keyValueLength = dataInputStream.readInt();
                byte[] keyValueBytes = dataInputStream.readNBytes(keyValueLength);
                entries.add(new WALEntry(entryIndex, keyValueBytes, timeStamp));
                logger.info("entryIndex: " + entryIndex + " timeStamp: " + timeStamp + " keyValueLength: " + Pair.deserialize(keyValueBytes));
            }
        } catch (IOException e) {
            logger.error("Error deserializing WALEntry", e);
        }
        return entries;
    }
}
