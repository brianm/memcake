package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class IncrementQuietlyCommand extends Command {
    private final CompletableFuture<Void> result;
    private final byte[] key;
    private final long delta;
    private final long initial;
    private final int expiration;
    private final Version cas;

    IncrementQuietlyCommand(CompletableFuture<Void> result,
                            byte[] key,
                            long delta,
                            long initial,
                            int expiration,
                            Version cas,
                            long timeout, TimeUnit timeoutUnit) {
        super(timeout, timeoutUnit);
        this.result = result;
        this.key = key;
        this.delta = delta;
        this.initial = initial;
        this.expiration = expiration;
        this.cas = cas;
    }
    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    byte extraLength() {
        return 20;
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.putLong(delta);
        buffer.putLong(initial);
        buffer.putInt(expiration);
        buffer.put(key);
    }

    @Override
    long cas() {
        return cas.token();
    }

    @Override
    byte opcode() {
        return Opcodes.incrementq;
    }
}
