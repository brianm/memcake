package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;

abstract class Command {

    private final long timeout;
    private final TimeUnit unit;

    protected Command(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    abstract Responder createResponder(int opaque);

    abstract void write(Connection conn, Integer opaque);

    static void writeBuffer(final Connection conn, final ByteBuffer buffer) {
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

    long getTimeout() {
        return timeout;
    }

    TimeUnit getUnit() {
        return unit;
    }

    boolean isQuiet() {
        return false;
    }
}
