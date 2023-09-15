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
        byte[] compressedKey = ByteUtils.intToBytes(key);
        byte[] compressedValueLength = ByteUtils.intToBytes(value.length());
        byte[] valueBytes = value.getBytes();
        int totalLength = compressedKey.length + compressedValueLength.length + valueBytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength);
        byteBuffer.put(compressedKey);
        byteBuffer.put(compressedValueLength);
        byteBuffer.put(valueBytes);
        return byteBuffer.array();
    }

    public static Pair deserialize(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int key = byteBuffer.getInt();
        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);
        String value = new String(valueBytes);
        return new Pair(key, value);
    }

}
