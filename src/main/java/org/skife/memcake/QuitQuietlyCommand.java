package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class QuitQuietlyCommand extends Command {

    private final CompletableFuture<Void> result;

    QuitQuietlyCommand(CompletableFuture<Void> result, long timeout, TimeUnit unit) {
        super(timeout, unit);
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
