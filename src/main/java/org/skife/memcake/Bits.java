package org.skife.memcake;

class Bits {
    static final byte CLIENT_MAGIC = (byte) 0x80;
    static final byte SERVER_MAGIC = (byte) 0x81;


    static byte[] charToBytes(int value) {
        return new byte[]{(byte) (value >> 8), (byte) value};
    }

    static byte[] intToBytes(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    static int toUnsignedInt(byte[] bytes) {
        if (bytes.length < 4) {
            throw new IllegalArgumentException("must pass at least 4 bytes");
        }
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }
}
