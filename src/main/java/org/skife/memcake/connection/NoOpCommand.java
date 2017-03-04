package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class NoOpCommand extends Command {
    private final CompletableFuture<Void> result;

    NoOpCommand(CompletableFuture<Void> result, Duration timeout) {
        super(timeout);
        this.result = result;
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    byte opcode() {
        return Opcodes.noop;
    }
}
