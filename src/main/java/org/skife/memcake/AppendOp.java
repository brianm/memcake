package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AppendOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;

    private Duration timeout;
    private Version version = Version.NONE;

    AppendOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public AppendOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public AppendOp cas(Version version) {
        this.version = version;
        return this;
    }

    public CompletableFuture<Version> execute() {
        return memcake.call(key, (c) -> c.append(key, value, version, timeout));
    }
}
