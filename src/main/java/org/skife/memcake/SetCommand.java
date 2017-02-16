package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Map;
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
    public Consumer<Map<Integer, Response>> createConsumer(int opaque) {
        return (s) -> {
            Response r = s.get(opaque);
            result.complete(new Version(r.getVersion()));
        };
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + 8 + key.length + value.length);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(Opcodes.set);
        buffer.putChar((char)key.length);
        buffer.put((byte)0x08); // extra length
        buffer.put((byte)0x00); // data type
        buffer.putChar((char)0x00); // vbucket
        buffer.putInt(8 + key.length + value.length);
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        buffer.putInt(flags);
        buffer.putInt(expires);
        buffer.put(key);
        buffer.put(value);

        buffer.flip();
        writeBuffer(conn, buffer);
    }

    private void writeBuffer(final Connection conn, final ByteBuffer buffer) {
        conn.getChannel().write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                if (attachment.remaining() != 0) {
                    writeBuffer(conn, buffer);
                    return;
                }
                conn.finishWrite();
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                conn.networkFailure(exc);
            }
        });
    }
}
