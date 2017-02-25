package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Value {
    private final Version cas;
    private final int flags;
    private final Optional<byte[]> key;
    private final byte[] value;

    Value(Version cas, int flags, Optional<byte[]> key, byte[] value) {
        this.cas = cas;
        this.flags = flags;
        this.key = key;
        this.value = value;
    }

    public Version getVersion() {
        return cas;
    }

    public int getFlags() {
        return flags;
    }

    public byte[] getValue() {
        return value;
    }

    public Optional<byte[]> getKey() {
        return key;
    }
}
