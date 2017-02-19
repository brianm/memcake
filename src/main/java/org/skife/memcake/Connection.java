package org.skife.memcake;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Connection implements AutoCloseable {

    private final BlockingDeque<Command> queuedRequests = new LinkedBlockingDeque<>();

    // made visible-ish for testing purposes only
    final ConcurrentMap<Integer, Responder> waiting = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer, Response> scoreboard = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer, Collection<Integer>> quietResponders = new ConcurrentHashMap<>();
    final BlockingQueue<Integer> queuedQuiets = new LinkedBlockingQueue<>();


    private final AtomicInteger opaques = new AtomicInteger(Integer.MIN_VALUE);

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean writing = new AtomicBoolean(false);

    private final AsynchronousSocketChannel channel;
    private final ScheduledExecutorService timeoutExecutor;
    private final long defaultTimeout;
    private final TimeUnit defaultTimeoutUnit;

    private Connection(AsynchronousSocketChannel channel,
                       ScheduledExecutorService timeoutExecutor,
                       long defaultTimeout,
                       TimeUnit defaultTimeoutUnit) {
        this.channel = channel;
        this.timeoutExecutor = timeoutExecutor;
        this.defaultTimeout = defaultTimeout;
        this.defaultTimeoutUnit = defaultTimeoutUnit;
    }

    public static CompletableFuture<Connection> open(SocketAddress addr,
                                                     AsynchronousSocketChannel channel,
                                                     ScheduledExecutorService timeoutExecutor,
                                                     long defaultTimeout,
                                                     TimeUnit defaultTimeoutUnit) throws IOException {
        final CompletableFuture<Connection> cf = new CompletableFuture<>();
        channel.connect(addr, channel, new CompletionHandler<Void, AsynchronousSocketChannel>() {
            @Override
            public void completed(Void result, AsynchronousSocketChannel channel) {
                final Connection conn = new Connection(channel, timeoutExecutor, defaultTimeout, defaultTimeoutUnit);
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

    private void maybeWrite() {
        if (writing.compareAndSet(false, true)) {
            // write next outbound command
            // we rely on writeFinished() being called to unset this flag
            final Command c = queuedRequests.poll();
            if (c == null) {
                writing.set(false);
                return;
            }

            int opaque = opaques.getAndIncrement();
            Responder responder = c.createResponder(opaque);
            waiting.put(opaque, responder);

            if (c.isQuiet()) {
                queuedQuiets.add(opaque);
                // TODO enqueue it somehow for later non-quiet
            } else {
                List<Integer> quiets = new ArrayList<>();
                queuedQuiets.drainTo(quiets);
                quietResponders.put(opaque, quiets);
            }

            c.write(this, opaque);
            timeoutExecutor.schedule(() -> {
                Responder r = waiting.remove(opaque);
                scoreboard.remove(opaque); // possible race between response coming in and timeout hitting
                r.failure(new TimeoutException());
            }, defaultTimeout, defaultTimeoutUnit);
        }
    }

    void finishWrite() {
        writing.set(false);
        maybeWrite();
    }

    void nextResponse(ByteBuffer headerBuffer) {
        if (open.get()) {
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
    }

    void networkFailure(Throwable exc) {
        close();
        waiting.forEach((_opaque, responder) -> responder.failure(exc));
        waiting.clear();
    }

    private void processResponseHeader(ByteBuffer buffer) {
        new Response(this, buffer).readBody();
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
            } catch (Exception e) {
                // close quietly
            }
        }
    }

    AsynchronousSocketChannel getChannel() {
        return channel;
    }

    void receive(Response response) {
        Responder sc = waiting.get(response.getOpaque());
        if (sc != null) {
            // only store on scoreboard if *something* is waiting for it
            scoreboard.put(response.getOpaque(), response);
            Integer opaque = sc.completed(scoreboard);
            scoreboard.remove(opaque);
            waiting.remove(opaque);
        }
        Collection<Integer> quiets = quietResponders.remove(response.getOpaque());
        if (quiets != null) {
            for (Integer quiet : quiets) {
                Responder r = waiting.remove(quiet);
                if (r != null) {
                    r.completed(scoreboard);
                }
                scoreboard.remove(quiet);
            }
        }
    }

    /* the main api of this thing */


    private <T> CompletableFuture<T> enqueue(CompletableFuture<T> result, Command c) {
        checkState();
        queuedRequests.add(c);
        maybeWrite();
        return result;
    }

    public CompletableFuture<Optional<Value>> get(byte[] key) {
        CompletableFuture<Optional<Value>> cf = new CompletableFuture<>();
        return enqueue(cf, new GetCommand(cf, key, defaultTimeout, defaultTimeoutUnit));
    }

    public CompletableFuture<Optional<Value>> getq(byte[] key) {
        CompletableFuture<Optional<Value>> cf = new CompletableFuture<>();
        return enqueue(cf, new GetQuietlyCommand(cf, key, defaultTimeout, defaultTimeoutUnit));
    }

    public CompletableFuture<Version> set(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> result = new CompletableFuture<>();
        return enqueue(result, new SetCommand(result, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit));
    }

    public CompletableFuture<Version> add(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> result = new CompletableFuture<>();
        return enqueue(result, new AddCommand(result, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit));
    }

    public CompletableFuture<Version> replace(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> result = new CompletableFuture<>();
        return enqueue(result,
                       new ReplaceCommand(result, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit));
    }

    public CompletableFuture<Void> flush(int expires) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        return enqueue(result, new FlushCommand(result, expires, defaultTimeout, defaultTimeoutUnit));
    }
}
