package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class SetCommand implements Command {
    private final CompletableFuture<Version> result;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;

    SetCommand(CompletableFuture<Version> result, byte[] key, int flags, int expires, byte[] value) {
        this.result = result;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
    }

    @Override
    public Optional<Responder> createResponder(int opaque) {
        return Optional.of(new Responder((s) -> {
            Response r = s.get(opaque);
            s.remove(opaque);

            if (r.getStatus() != 0) {
                result.completeExceptionally(new StatusException(r.getStatus(), r.getError()));
            }
            else {
                result.complete(new Version(r.getVersion()));
            }
        }, result::completeExceptionally));
    }

    protected byte opCode() {
        return Opcodes.set;
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        int bodyLength = 8 + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(24 + bodyLength);

        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(opCode());
        buffer.putChar((char) key.length);
        buffer.put((byte) 0x08); // extra length
        buffer.put((byte) 0x00); // data type
        buffer.putChar((char) 0x00); // vbucket
        buffer.putInt(bodyLength);
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        buffer.putInt(flags);
        buffer.putInt(expires);
        buffer.put(key);
        buffer.put(value);

        buffer.flip();
        Command.writeBuffer(conn, buffer);
    }
}
