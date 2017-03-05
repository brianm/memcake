package org.skife.memcake.testing;

import com.google.common.base.MoreObjects;

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("key", key)
                          .add("value", value)
                          .toString();
    }
}
