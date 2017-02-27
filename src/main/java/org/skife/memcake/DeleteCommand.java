package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DeleteCommand extends Command {

    private final CompletableFuture<Void> result;
    private final byte[] key;
    private final Version cas;

    DeleteCommand(CompletableFuture<Void> result,
                  byte[] key,
                  Version cas,
                  long timeout,
                  TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
        this.cas = cas;
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.put(key);
    }

    @Override
    byte opcode() {
        return Opcodes.delete;
    }

    @Override
    long cas() {
        return cas.token();
    }
}
