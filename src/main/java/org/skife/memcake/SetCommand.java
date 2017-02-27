package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class SetCommand extends Command {
    private final CompletableFuture<Version> result;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;
    private final Version casToken;

    SetCommand(CompletableFuture<Version> result,
               byte[] key,
               int flags,
               int expires,
               byte[] value,
               Version casToken,
               long timeout,
               TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
        this.casToken = casToken;
    }

    @Override
    public Responder createResponder(int opaque) {
        return Responder.versionResponder(result, opaque);
    }

    @Override
    long cas() {
        return casToken.token();
    }

    @Override
    byte extraLength() {
        return 8;
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    int bodyLength() {
        return value.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.putInt(flags);
        buffer.putInt(expires);
        buffer.put(key);
        buffer.put(value);
    }

    @Override
    byte opcode() {
        return Opcodes.set;
    }
}
