package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class DecrementQuietOp {
    private final Memcake memcake;
    private final byte[] key;
    private final int delta;
    private Duration timeout;
    private int initialValue;
    private int expires = 0xFFFFFFFF;
    private Version cas = Version.NONE;

    DecrementQuietOp(Memcake memcake, byte[] key, int delta, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.delta = delta;
        this.timeout = timeout;
    }

    public DecrementQuietOp initialValue(int initial) {
        this.initialValue = initial;
        this.expires = 0;
        return this;
    }

    public DecrementQuietOp cas(Version cas) {
        this.cas = cas;
        return this;
    }

    public DecrementQuietOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public DecrementQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.decrementq(key, delta, initialValue, expires, cas, timeout));
    }
}
