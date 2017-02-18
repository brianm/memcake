package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

interface Command {
    Optional<Consumer<Map<Integer, Response>>> createConsumer(int opaque);

    void write(Connection conn, Integer opaque);

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
}
