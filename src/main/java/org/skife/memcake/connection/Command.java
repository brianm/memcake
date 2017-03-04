package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.time.Duration;

abstract class Command {

    private final Duration timeout;

    Command(Duration timeout) {
        this.timeout = timeout;
    }

    abstract Responder createResponder(int opaque);

    byte extraLength() {
        return 0;
    }

    char keyLength() {
        return 0;
    }

    int bodyLength() {
        return 0;
    }

    long cas() {
        return 0;
    }

    void writeBody(ByteBuffer buffer) {
    }

    final void write(Connection conn, Integer opaque) {
        byte extraLength = extraLength();
        char keyLength = keyLength();
        int bodyLength = bodyLength();

        ByteBuffer buffer = ByteBuffer.allocate(24 + extraLength + keyLength + bodyLength);
        buffer.put((byte) 0x80); // client magic number
        buffer.put(opcode());
        buffer.putChar(keyLength);
        buffer.put(extraLength); // extra length
        buffer.put((byte) 0x00); // data type
        buffer.putChar((char) 0x00); // vbucket
        buffer.putInt(extraLength + keyLength + bodyLength); // totalBody
        buffer.putInt(opaque);
        buffer.putLong(cas());

        writeBody(buffer);

        buffer.flip();
        Command.writeBuffer(conn, buffer);
    }

    private static void writeBuffer(final Connection conn, final ByteBuffer buffer) {
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

    Duration getTimeout() {
        return timeout;
    }

    boolean isQuiet() {
        return false;
    }

    abstract byte opcode();
}
