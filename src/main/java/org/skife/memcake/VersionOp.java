package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class VersionOp {
    private final Memcake memcake;
    private Duration timeout;

    VersionOp(Memcake memcake, Duration timeout) {
        this.memcake = memcake;
        this.timeout = timeout;
    }

    public VersionOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<String> execute() {
        // TODO what to do about multiple servers?
        return memcake.call(null, (c) -> c.version(timeout));
    }
}
