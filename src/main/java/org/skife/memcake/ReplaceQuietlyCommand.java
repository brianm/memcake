package org.skife.memcake;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class ReplaceQuietlyCommand extends SetQuietlyCommand {
    ReplaceQuietlyCommand(CompletableFuture<Void> result,
                                 byte[] key,
                                 int flags,
                                 int expires,
                                 byte[] value,
                                 Optional<Version> cas, long timeout, TimeUnit timeoutUnit) {
        super(result, key, flags, expires, value, cas, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.replaceq;
    }
}
