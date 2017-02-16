package org.skife.memcake;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Connection implements AutoCloseable {

    private final BlockingDeque<Command> queuedRequests = new LinkedBlockingDeque<>();

    private final ConcurrentMap<Integer, Consumer<Map<Integer, Response>>> waiting = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Response> scoreboard = new ConcurrentHashMap<>();

    private final AtomicInteger opaques = new AtomicInteger(Integer.MIN_VALUE);

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean writing = new AtomicBoolean(false);

    private final AsynchronousSocketChannel channel;

    private Connection(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    void maybeWrite() {
        if (writing.compareAndSet(false, true)) {
            // write next outbound command
            // we rely on writeFinished() being called to unset this flag
            Command c = queuedRequests.poll();
            if (c == null) {
                writing.set(false);
                return;
            }

            int opaque = opaques.getAndIncrement();
            waiting.put(opaque, c.createConsumer(opaque));
            c.write(this, opaque);

        }
    }

    void finishWrite() {
        writing.set(false);
        maybeWrite();
    }

    void nextResponse(ByteBuffer headerBuffer) {
        channel.read(headerBuffer, headerBuffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer bytesRead, ByteBuffer buffer) {
                if (buffer.remaining() != 0) {
                    nextResponse(buffer);
                    return;
                }
                buffer.flip();
                processResponseHeader(buffer);
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                networkFailure(exc);
            }
        });
    }

    void networkFailure(Throwable exc) {
        close();
        throw new UnsupportedOperationException("Not Yet Implemented!", exc);
    }

    private void processResponseHeader(ByteBuffer buffer) {
        new Response(this, buffer).readBody();
    }

    public static CompletableFuture<Connection> open(SocketAddress addr,
                                                     AsynchronousChannelGroup group) throws IOException {
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
        final CompletableFuture<Connection> cf = new CompletableFuture<>();
        channel.connect(addr, channel, new CompletionHandler<Void, AsynchronousSocketChannel>() {
            @Override
            public void completed(Void result, AsynchronousSocketChannel channel) {
                Connection conn = new Connection(channel);
                conn.nextResponse(ByteBuffer.allocate(24));
                cf.complete(conn);
            }

            @Override
            public void failed(Throwable exc, AsynchronousSocketChannel attachment) {
                cf.completeExceptionally(exc);
            }
        });
        return cf;
    }

    private void checkState() {
        if (!open.get()) {
            throw new IllegalStateException("Connection is closed and no longer usable");
        }
    }

    public void close() {
        if (open.compareAndSet(true, false)) {
            try {
                channel.close();
            } catch (IOException e) {
                // close quietly
            }
        }
    }

    /* the main api of this thing */

    public CompletableFuture<Version> set(byte[] key, int flags, int expires, byte[] value) {
        checkState();
        CompletableFuture<Version> result = new CompletableFuture<>();
        queuedRequests.add(new SetCommand(result, key, flags, expires, value));
        maybeWrite();
        return result;
    }

    public CompletableFuture<Value> get(byte[] key) {
        checkState();
        CompletableFuture<Value> result = new CompletableFuture<>();
        queuedRequests.add(new GetCommand(result, key));
        maybeWrite();
        return result;
    }

    AsynchronousSocketChannel getChannel() {
        return channel;
    }

    void receive(Response response) {
        scoreboard.put(response.getOpaque(), response);
        Consumer<Map<Integer, Response>> sc = waiting.get(response.getOpaque());
        if (sc != null) {
            sc.accept(scoreboard);
        }
    }
}
