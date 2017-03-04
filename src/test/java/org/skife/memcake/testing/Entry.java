package org.skife.memcake.testing;

public class Entry {
    private final byte[] key;
    private final byte[] value;

    public Entry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }
}
