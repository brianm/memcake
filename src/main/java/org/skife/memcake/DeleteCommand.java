package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DeleteCommand extends Command {

    private final CompletableFuture<Void> result;
    private final byte[] key;

    DeleteCommand(CompletableFuture<Void> result, byte[] key, long timeout, TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
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
}
