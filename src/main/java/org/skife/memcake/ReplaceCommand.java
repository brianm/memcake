package org.skife.memcake;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class ReplaceCommand extends SetCommand {

    ReplaceCommand(CompletableFuture<Version> result,
                   byte[] key,
                   int flags,
                   int expires,
                   byte[] value,
                   Version casToken,
                   long timeout,
                   TimeUnit unit) {
        super(result, key, flags, expires, value, casToken, timeout, unit);
    }

    @Override
    protected byte opcode() {
        return Opcodes.replace;
    }
}
