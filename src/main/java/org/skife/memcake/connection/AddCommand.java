package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class AddCommand extends SetCommand {
    AddCommand(CompletableFuture<Version> result,
               byte[] key,
               int flags,
               int expires,
               byte[] value,
               Duration timeout) {
        super(result, key, flags, expires, value, Version.ZERO, timeout);
    }

    @Override
    protected byte opcode() {
        return Opcodes.add;
    }
}
