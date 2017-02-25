package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class FlushQuietlyCommand extends FlushCommand {
    FlushQuietlyCommand(CompletableFuture<Void> result,
                               int expires,
                               long timeout,
                               TimeUnit timeoutUnit) {
        super(result, expires, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.flushq;
    }

    @Override
    boolean isQuiet() {
        return true;
    }
}
