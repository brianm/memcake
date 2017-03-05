package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class DeleteOp {
    private final Memcake memcake;
    private final byte[] key;

    private Duration timeout;
    private Version cas = Version.NONE;

    DeleteOp(Memcake memcake, byte[] key, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.timeout = timeout;
    }

    public DeleteOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public DeleteOp cas(Version version) {
        this.cas = version;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.delete(key, cas, timeout));
    }
}
