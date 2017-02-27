package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class AddQuietCommand extends SetQuietCommand {

    AddQuietCommand(CompletableFuture<Void> result,
                    byte[] key,
                    int flags,
                    int expires,
                    byte[] value,
                    long timeout, TimeUnit timeoutUnit) {
        super(result, key, flags, expires, value, Version.ZERO, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.addq;
    }
}
