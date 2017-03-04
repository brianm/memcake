package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class QuitQuietlyCommand extends Command {

    private final CompletableFuture<Void> result;

    QuitQuietlyCommand(CompletableFuture<Void> result, Duration timeout) {
        super(timeout);
        this.result = result;
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    byte opcode() {
        return Opcodes.quitq;
    }
}
