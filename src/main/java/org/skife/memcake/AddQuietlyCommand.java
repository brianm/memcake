package org.skife.memcake;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class AddQuietlyCommand extends SetQuietlyCommand {

    AddQuietlyCommand(CompletableFuture<Void> result,
                             byte[] key,
                             int flags,
                             int expires,
                             byte[] value,
                             long timeout, TimeUnit timeoutUnit) {
        super(result, key, flags, expires, value, Optional.empty(), timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.addq;
    }
}
