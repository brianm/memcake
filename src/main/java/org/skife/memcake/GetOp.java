package org.skife.memcake;

import org.skife.memcake.connection.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class GetOp {
    private final Memcake memcake;
    private final byte[] key;
    private Duration timeout;

    GetOp(Memcake memcake, byte[] key, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.timeout = timeout;
    }

    public GetOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }


    public CompletableFuture<Optional<Value>> execute() {
        return memcake.call(key, (c) -> c.get(key, timeout));
    }
}
