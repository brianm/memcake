package org.skife.memcake;

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AddOp extends SetOp {

    AddOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        super(memcake, key, value, timeout);
    }

    @Override
    public CompletableFuture<Version> execute() {
        return memcake.call(key, (c) -> c.add(key, flags, expires, value, timeout));
    }
}
