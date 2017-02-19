package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class FlushCommand extends Command {

    private final CompletableFuture<Void> result;
    private final int expires;

    FlushCommand(CompletableFuture<Void> result, int expires, long timeout, TimeUnit unit) {
        super(timeout, unit);
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
