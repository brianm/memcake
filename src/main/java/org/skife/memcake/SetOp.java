package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class SetOp {
    protected final Memcake memcake;

    protected final byte[] key;
    protected final byte[] value;
    protected int expires = 0;
    protected int flags = 0;
    protected Version version = Version.NONE;
    protected Duration timeout;

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
