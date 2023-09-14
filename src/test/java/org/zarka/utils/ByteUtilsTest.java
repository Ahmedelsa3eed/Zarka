package org.zarka.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteUtilsTest {

    @Test
    void compressInt() {
        byte[] compressed = ByteUtils.compressInt(1);
        assertEquals(1, ByteUtils.bytesToInt(compressed));
    }

    @Test
    void compressIntTwo() {
        byte[] compressed = ByteUtils.compressInt(-1);
        assertEquals(-1, ByteUtils.bytesToInt(compressed));
    }

    @Test
    void compressInt3() {
        byte[] compressed = ByteUtils.compressInt(165189);
        assertEquals(165189, ByteUtils.bytesToInt(compressed));
    }

    @Test
    void compressInt4() {
        byte[] compressed = ByteUtils.compressInt(-165189);
        assertEquals(-165189, ByteUtils.bytesToInt(compressed));
    }
}