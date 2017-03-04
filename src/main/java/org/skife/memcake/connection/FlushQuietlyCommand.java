package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class FlushQuietlyCommand extends FlushCommand {
    FlushQuietlyCommand(CompletableFuture<Void> result,
                               int expires,
                        Duration timeout) {
        super(result, expires, timeout);
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
