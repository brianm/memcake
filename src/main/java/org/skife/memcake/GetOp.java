package org.skife.memcake;

import org.skife.memcake.connection.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class GetOp {
    private final Memcake memcake;
    private final byte[] key;

    private Optional<Duration> timeout = Optional.empty();

    GetOp(Memcake memcake, byte[] key) {
        this.memcake = memcake;
        this.key = key;
    }

    public GetOp timeout(Duration timeout) {
        this.timeout = Optional.of(timeout);
        return this;
    }


    public CompletableFuture<Optional<Value>> execute() {
        return memcake.perform(key, (c) -> {
            return c.get(key, timeout.orElse(c.getDefaultTimeout()));
        });
    }
}
