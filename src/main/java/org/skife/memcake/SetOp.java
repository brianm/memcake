package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class SetOp {
    private final Memcake memcake;

    private final byte[] key;
    private final byte[] value;
    private int expires = 0;
    private int flags = 0;
    private Version version = Version.NONE;
    private Duration timeout;

    SetOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
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
        this.timeout = timeout;
        return this;
    }

    public SetOp version(Version version) {
        this.version = version;
        return this;
    }

    public CompletableFuture<Version> execute() {
        return memcake.call(key, (c) -> c.set(key, flags, expires, value, version, timeout));
    }
}
