package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class AppendQuietlyCommand extends Command {
    private final CompletableFuture<Void> result;
    private final byte[] key;
    private final byte[] value;
    private final Version cas;

    AppendQuietlyCommand(CompletableFuture<Void> result,
                         byte[] key,
                         byte[] value,
                         Version cas,
                         long timeout,
                         TimeUnit timeoutUnit) {
        super(timeout, timeoutUnit);
        this.result = result;
        this.key = key;
        this.value = value;
        this.cas = cas;
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, result, opaque);
    }

    @Override
    byte opcode() {
        return Opcodes.appendq;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.put(key);
        buffer.put(value);
    }

    @Override
    int bodyLength() {
        return value.length;
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    long cas() {
        return cas.token();
    }
}
