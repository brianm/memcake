package org.skife.memcake;

public class Value {
    private final Version cas;
    private final int flags;
    private final byte[] value;

    public Value(Version cas, int flags, byte[] value) {
        this.cas = cas;
        this.flags = flags;
        this.value = value;
    }

    public Version getCas() {
        return cas;
    }

    public int getFlags() {
        return flags;
    }

    public byte[] getValue() {
        return value;
    }
}
