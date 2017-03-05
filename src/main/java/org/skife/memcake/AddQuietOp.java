package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AddQuietOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;
    private Duration timeout;
    
    private int expires = 0;
    private int flags = 0;

    AddQuietOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public AddQuietOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public AddQuietOp flags(int flags) {
        this.flags = flags;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.addq(key, flags, expires, value, timeout));
    }

    public AddQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }
}
