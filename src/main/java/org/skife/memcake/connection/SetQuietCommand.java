package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class SetQuietCommand extends Command {
    private final CompletableFuture<Void> r;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;
    private final Version cas;

    SetQuietCommand(CompletableFuture<Void> r,
                    byte[] key,
                    int flags,
                    int expires,
                    byte[] value,
                    Version cas,
                    Duration timeout) {
        super(timeout);
        this.r = r;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
        this.cas = cas;
    }

    @Override
    long cas() {
        return cas.token();
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, r, opaque);
    }

    @Override
    boolean isQuiet() {
        return true;
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
        return Opcodes.setq;
    }
}
