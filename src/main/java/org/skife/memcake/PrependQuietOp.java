package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class PrependQuietOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;
    private Duration timeout;
    private Version cas = Version.NONE;

    PrependQuietOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public PrependQuietOp cas(Version version) {
        this.cas = version;
        return this;
    }

    public PrependQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.prependq(key, value, cas, timeout));
    }
}
