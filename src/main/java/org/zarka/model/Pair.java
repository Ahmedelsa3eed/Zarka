package org.zarka.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.utils.ByteUtils;

import java.io.*;
import java.nio.ByteBuffer;

public record Pair(Integer key, String value) implements Serializable {
    private static Logger logger = LogManager.getLogger(Pair.class);

    /**
     * total bytes = 4 (key) + 4 (value length) + value length
     * */
    public byte[] serialize() {
        byte[] compressedKey = ByteUtils.compressInt(key);
        byte[] compressedValueLength = ByteUtils.compressInt(value.length());
        byte[] valueBytes = value.getBytes();
        int totalLength = compressedKey.length + compressedValueLength.length + valueBytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength);
        byteBuffer.put(compressedKey);
        byteBuffer.put(compressedValueLength);
        byteBuffer.put(valueBytes);
        return byteBuffer.array();
    }

    public static Pair deserialize(InputStream is) {
        try {
            DataInputStream dis = new DataInputStream(is);
            Integer key = dis.readInt();
            int valueLength = dis.readInt();
            byte[] valueBytes = dis.readNBytes(valueLength);
            String value = new String(valueBytes);
            return new Pair(key, value);
        } catch (IOException e) {
            logger.error("Error deserializing K-Value pair", e);
            throw new RuntimeException(e);
        }
    }
}
