package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class GetCommand implements Command {

    private final CompletableFuture<Value> result;
    private final byte[] key;

    GetCommand(CompletableFuture<Value> result, byte[] key) {
        this.result = result;
        this.key = key;
    }

    @Override
    public Consumer<Map<Integer, Response>> createConsumer(int opaque) {
        return (s) -> {
            Response r = s.get(opaque);
            s.remove(opaque);
            result.complete(new Value(new Version(r.getVersion()), r.getFlags(), r.getValue()));
        };
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + 8 + key.length);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(Opcodes.get);
        buffer.putChar((char)key.length);
        buffer.put((byte)0x00); // extra length
        buffer.put((byte)0x00); // data type
        buffer.putChar((char)0x00); // vbucket
        buffer.putInt(key.length);
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        buffer.put(key);

        buffer.flip();
        Command.writeBuffer(conn, buffer);
    }
}
