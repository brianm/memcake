package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class AppendCommand extends Command {

    private final CompletableFuture<Version> result;
    private final byte[] key;
    private final byte[] value;
    private final Version cas;

    AppendCommand(CompletableFuture<Version> result,
                  byte[] key,
                  byte[] value,
                  Version cas,
                  long timeout,
                  TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
        this.value = value;
        this.cas = cas;
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
    Responder createResponder(int opaque) {
        return Responder.versionResponder(result, opaque);
    }

    @Override
    byte opcode() {
        return Opcodes.append;
    }

    @Override
    long cas() {
        return cas.token();
    }
}
