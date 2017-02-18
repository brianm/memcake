package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class AddCommand implements Command {
    private final CompletableFuture<Version> result;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;

    AddCommand(CompletableFuture<Version> result, byte[] key, int flags, int expires, byte[] value) {
        this.result = result;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
    }

    @Override
    public Optional<Consumer<Map<Integer, Response>>> createConsumer(int opaque) {
        return Optional.of((s) -> {
            Response r = s.get(opaque);
            s.remove(opaque);
            result.complete(new Version(r.getVersion()));
        });
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        int bodyLength = 8 + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(24 + bodyLength);

        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(Opcodes.add);
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
