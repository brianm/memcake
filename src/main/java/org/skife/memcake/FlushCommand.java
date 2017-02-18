package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class FlushCommand implements Command {

    private final CompletableFuture<Void> result;
    private final int expires;

    FlushCommand(CompletableFuture<Void> result, int expires) {
        this.result = result;
        this.expires = expires;
    }

    @Override
    public Optional<Consumer<Map<Integer, Response>>> createConsumer(int opaque) {
        return Optional.of((s) -> {
            Response r = s.get(opaque);
            s.remove(opaque);
            if (r.getStatus() != 0) {
                result.completeExceptionally(new StatusException(r.getStatus(), r.getError()));
            } else {
                result.complete(null);
            }
        });
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        byte extraLength = (byte) (expires > 0 ? 4 : 0);
        ByteBuffer buffer = ByteBuffer.allocate(24 + extraLength);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(Opcodes.flush);
        buffer.put((byte) 0x00); // key length 1
        buffer.put((byte) 0x00); // key length 1
        buffer.put(extraLength); // extra length
        buffer.put((byte) 0x00); // data type
        buffer.putChar((char) 0x00); // vbucket
        buffer.putInt(extraLength); // totalBody
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        if (expires > 0) {
            buffer.putInt(expires);
        }

        buffer.flip();
        Command.writeBuffer(conn, buffer);

    }

    static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        conn.receive(response);
    }
}
