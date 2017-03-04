package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class IncrementCommand extends Command {

    private final CompletableFuture<Counter> result;
    private final byte[] key;
    private final long delta;
    private final long initial;
    private final int expiration;
    private final Version cas;

    IncrementCommand(CompletableFuture<Counter> result,
                     byte[] key,
                     long delta,
                     long initial,
                     int expiration,
                     Version cas,
                     Duration timeout) {
        super(timeout);
        this.result = result;
        this.key = key;
        this.delta = delta;
        this.initial = initial;
        this.expiration = expiration;
        this.cas = cas;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);
            byte[] vbytes = r.getValue();
            if (vbytes.length != 8) {
                result.completeExceptionally(new IllegalStateException("counter value was not a long (8 bytes!)"));
                return;
            }

            Counter c = new Counter(ByteBuffer.wrap(vbytes).getLong(), new Version(r.getVersion()));
            result.complete(c);
        });
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
        return Opcodes.increment;
    }

    public static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        byte[] value = new byte[response.getTotalBodyLength() - response.getKeyLength() - response.getExtrasLength()];
        bodyBuffer.get(value);
        response.setValue(value);
        conn.receive(response);
    }
}