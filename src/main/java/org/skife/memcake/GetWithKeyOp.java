package org.skife.memcake;

import org.skife.memcake.connection.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class GetWithKeyOp {
    private final Memcake memcake;
    private final byte[] key;
    private Duration timeout;

    GetWithKeyOp(Memcake memcake, byte[] key, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.timeout = timeout;
    }

    public GetWithKeyOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Optional<Value>> execute() {
        return memcake.call(key, (c) -> c.getk(key, timeout));
    }
}
