package org.skife.memcake.connection;

public class Counter {

    private final long value;
    private final Version version;

    Counter(long value, Version version) {
        this.value = value;
        this.version = version;
    }

    public long getValue() {
        return value;
    }

    public Version getVersion() {
        return version;
    }
}
