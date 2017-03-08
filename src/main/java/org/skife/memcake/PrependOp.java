package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class PrependOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;
    private Duration timeout;
    private Version version = Version.NONE;

    PrependOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public PrependOp cas(Version version) {
        this.version = version;
        return this;
    }

    public PrependOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Version> execute() {
        return memcake.call(key, (c) -> c.prepend(key, value, version, timeout));
    }
}
