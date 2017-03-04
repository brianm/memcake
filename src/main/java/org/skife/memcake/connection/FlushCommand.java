package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class FlushCommand extends Command {

    private final CompletableFuture<Void> result;
    private final int expires;

    FlushCommand(CompletableFuture<Void> result, int expires, Duration timeout) {
        super(timeout);
        this.result = result;
        this.expires = expires;
    }

    @Override
    public Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    byte extraLength() {
        return (byte) (expires > 0 ? 4 : 0);
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        if (expires >0) {
            buffer.putInt(expires);
        }
    }

    @Override
    byte opcode() {
        return Opcodes.flush;
    }
}
