package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

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

    public static WALEntry deserialize(InputStream is) {
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            long entryIndex = dataInputStream.readLong();
            long timeStamp = dataInputStream.readLong();
            int keyValueLength = dataInputStream.readInt();
            byte[] keyValueBytes = dataInputStream.readNBytes(keyValueLength);

            return new WALEntry(entryIndex, keyValueBytes, timeStamp);
        } catch (IOException e) {
            logger.error("Error deserializing WALEntry", e);
            throw new RuntimeException(e);
        }
    }
}
