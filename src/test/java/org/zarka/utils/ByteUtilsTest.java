package org.zarka.utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ByteUtilsTest {

    @Test
    void compressInt() {
        byte[] compressed = ByteUtils.intToBytes(1);
        assertEquals(1, ByteBuffer.wrap(compressed).getInt());
    }

    @Test
    void compressIntTwo() {
        byte[] compressed = ByteUtils.intToBytes(-1);
        assertEquals(-1, ByteBuffer.wrap(compressed).getInt());
    }

    @Test
    void compressInt3() {
        byte[] compressed = ByteUtils.intToBytes(165189);
        assertEquals(165189, ByteBuffer.wrap(compressed).getInt());
    }

    @Test
    void compressInt4() {
        byte[] compressed = ByteUtils.intToBytes(-165189);
        assertEquals(-165189, ByteBuffer.wrap(compressed).getInt());
    }
}