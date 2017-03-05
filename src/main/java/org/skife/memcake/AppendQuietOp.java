package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AppendQuietOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;

    private Duration timeout;
    private Version cas = Version.NONE;

    AppendQuietOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public AppendQuietOp cas(Version version) {
        this.cas = version;
        return this;
    }

    public AppendQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.appendq(key, value, cas, timeout));
    }
}
