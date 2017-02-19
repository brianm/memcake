package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class ReplaceCommand extends SetCommand {
    ReplaceCommand(CompletableFuture<Version> result,
                   byte[] key,
                   int flags,
                   int expires,
                   byte[] value,
                   long timeout,
                   TimeUnit unit) {
        super(result, key, flags, expires, value, timeout, unit);
    }

    @Override
    protected byte opCode() {
        return Opcodes.replace;
    }
}
