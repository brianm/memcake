package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class SetOp {
    private final Memcake memcake;

    private final byte[] key;
    private final byte[] value;

    private int expires = 0;
    private int flags = 0;
    private Version version = Version.NONE;
    private Optional<Duration> timeout = Optional.empty();

    SetOp(Memcake memcake, byte[] key, byte[] value) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
    }

    public SetOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public SetOp flags(int flags) {
        this.flags = flags;
        return this;
    }

    public SetOp timeout(Duration timeout) {
        this.timeout = Optional.of(timeout);
        return this;
    }

    public SetOp version(Version version) {
        this.version = version;
        return this;
    }

    public CompletableFuture<Version> execute() {
        return memcake.perform(key, (c) -> {
            return c.set(key, flags, expires, value, version, timeout.orElse(c.getDefaultTimeout()));
        });
    }
}
