package org.skife.memcake.connection;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.time.Duration;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Connection implements AutoCloseable {

    // TODO put a limit on how deep this can grow!
    private final BlockingDeque<Pair<Long, Command>> queuedRequests = new LinkedBlockingDeque<>();

    // made visible-ish for white box testing purposes only
    final ConcurrentMap<Integer, Responder> waiting = new ConcurrentHashMap<>();

    // opaque -> Response mapping for responses which have not been responded to yet.
    // Will generally cycle quickly, the only time it should accumulate is when a big
    // pipeline of quiet gets (or such) were fired off and we're waiting on their
    // anchor to land.
    // made visible-ish for white box testing purposes only
    final ConcurrentMap<Integer, Response> scoreboard = new ConcurrentHashMap<>();

    // list of opaques for quiet operations which have been written but which have not had
    // a non-quiet command follow yet. Queue will be dumped into quietProxies once such
    // a command is sent.
    // made visible-ish for white box testing purposes only
    // TODO consider putting a limit on how deep this can grow by forcing a NOOP
    //      command once it hits that limit.
    final BlockingQueue<Integer> queuedQuiets = new LinkedBlockingQueue<>();

    // tracks the collection of of quiet command opaques which were sent after the last non-quiet
    // command, and before the key here. Basically, when we see the opaque for any of these
    // keys come back, we know the commands from the opaques attached have all been processed
    // so if we didn't get a response, well, make sense of it based on the type of quiet :-)
    // made visible-ish for white box testing purposes only
    final ConcurrentMap<Integer, Collection<Integer>> quietProxies = new ConcurrentHashMap<>();

    private final AtomicInteger opaques = new AtomicInteger(Integer.MIN_VALUE);

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean writing = new AtomicBoolean(false);
    private final List<Runnable> networkFailureListeners = new CopyOnWriteArrayList<>();

    private final AsynchronousByteChannel channel;
    private final ScheduledExecutorService timeoutExecutor;

    Connection(AsynchronousByteChannel channel,
               ScheduledExecutorService timeoutExecutor) {
        this.channel = channel;
        this.timeoutExecutor = timeoutExecutor;
    }

    public static CompletableFuture<Connection> open(SocketAddress memcachedServerAddress,
                                                     AsynchronousSocketChannel channel,
                                                     ScheduledExecutorService timeoutExecutor) throws IOException {
        final CompletableFuture<Connection> cf = new CompletableFuture<>();
        channel.connect(memcachedServerAddress, channel, new CompletionHandler<Void, AsynchronousSocketChannel>() {
            @Override
            public void completed(Void result, AsynchronousSocketChannel channel) {
                final Connection conn = new Connection(channel, timeoutExecutor);
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

    /**
     * The main write loop used to ensure only one write is happening at a time.
     * <p>
     * Mutually recursive with finishWrite via {@link Command#write(Connection, Integer)}
     */
    private void maybeWrite() {
        if (writing.compareAndSet(false, true)) {
            // write next outbound command
            // we rely on writeFinished() being called to unset this flag
            final Pair<Long, Command> cp = queuedRequests.poll();
            if (cp == null) {
                writing.set(false);
                return;
            }
            Command c = cp.right();
            int opaque = opaques.getAndIncrement();
            Responder responder = c.createResponder(opaque);
            waiting.put(opaque, responder);

            if (c.isQuiet()) {
                queuedQuiets.add(opaque);
                // TODO enqueue it somehow for later non-quiet
            }
            else {
                List<Integer> quiets = new ArrayList<>();
                queuedQuiets.drainTo(quiets);
                quietProxies.put(opaque, quiets);
            }

            c.write(this, opaque);
            long timeoutNanos = c.getTimeout().toNanos();
            long queueTime = System.nanoTime() - cp.left();
            // c.getTimeoutUnit().convert(System.nanoTime() - cp.left(), TimeUnit.NANOSECONDS);

            timeoutExecutor.schedule(() -> {
                Responder r = waiting.remove(opaque);
                scoreboard.remove(opaque); // possible race between response coming in and timeout hitting
                r.failure(new TimeoutException("timed out after " + c.getTimeout()));
            }, timeoutNanos - queueTime, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Called by commands when the finish writing themselves out, this is the recur bit of the
     * main write loop, paired with maybeWrite()
     */
    void finishWrite() {
        writing.set(false);
        maybeWrite();
    }

    /**
     * Main read loop.
     */
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
                    // creating the response parses out the buffer
                    // readBody may queue additional read operations.
                    // when any reads queued by readBody() complete, it
                    // invokes nextReponse again.
                    new Response(Connection.this, buffer).readBody();
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    networkFailure(exc);
                }
            });
        }
    }

    /**
     * Invoked if any networking operations hit an error.
     */
    void networkFailure(Throwable exc) {
        if (failed.compareAndSet(false, true)) {
            close();
            waiting.forEach((_opaque, responder) -> responder.failure(exc));
            waiting.clear();
            for (Runnable listener : networkFailureListeners) {
                listener.run();
            }
        }
    }

    public boolean isOpen() {
        return open.get();
    }

    public void close() {
        if (open.compareAndSet(true, false)) {
            try {
                quit(Duration.ZERO).get();
            } catch (Exception e) {
                // close quietly
            }
            try {
                channel.close();
            } catch (IOException e) {
                // close quietly
            }
        }
    }

    private void checkState() {
        if (!open.get()) {
            throw new IllegalStateException("Connection is closed and no longer usable");
        }
    }

    AsynchronousByteChannel getChannel() {
        return channel;
    }

    /**
     * Fully parsed response has been received, let's trigger listeners, etc.
     */
    void receive(Response response) {
        Responder sc = waiting.get(response.getOpaque());
        if (sc != null) {
            // only store on scoreboard if *something* is waiting for it
            scoreboard.put(response.getOpaque(), response);
            Integer opaque = sc.completed(scoreboard);
            scoreboard.remove(opaque);
            if (response.getOpcode() == Opcodes.stat) {
                // stat is a pain, multiple reponses come back for single opaque
                // so we need to special case it. BLARGH
                if (response.getKey() == null || response.getKey().length == 0) {
                    // there are more responses coming for this stat, DO NOT remove the waiting
                    waiting.remove(opaque);
                }
            }
            else {
                waiting.remove(opaque);
            }
        }
        Collection<Integer> quiets = quietProxies.remove(response.getOpaque());
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

    private <T> CompletableFuture<T> enqueue(Command command, CompletableFuture<T> result) {
        checkState();
        queuedRequests.add(Pair.of(System.nanoTime(), command));
        maybeWrite();
        return result;
    }

    /* the main api of this thing, as used by users */

    //
    public CompletableFuture<Optional<Value>> get(byte[] key, Duration timeout) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetCommand(r, key, timeout), r);
    }

    public CompletableFuture<Optional<Value>> getk(byte[] key, Duration timeout) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetKCommand(r, key, timeout), r);
    }

    public CompletableFuture<Optional<Value>> getkq(byte[] key, Duration timeout) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetKQuietCommand(r, key, timeout), r);
    }

    public CompletableFuture<Optional<Value>> getq(byte[] key, Duration timeout) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetQuietlyCommand(r, key, timeout), r);
    }

    public CompletableFuture<Void> setq(byte[] key,
                                        int flags,
                                        int expires,
                                        byte[] value,
                                        Version cas,
                                        Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new SetQuietCommand(r, key, flags, expires, value, cas, timeout), r);
    }

    //
    public CompletableFuture<Version> set(byte[] key,
                                          int flags,
                                          int expires,
                                          byte[] value,
                                          Version cas,
                                          Duration timeout) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new SetCommand(r, key, flags, expires, value, cas, timeout), r);
    }

    public CompletableFuture<Version> add(byte[] key, int flags, int expires, byte[] value, Duration timeout) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new AddCommand(r, key, flags, expires, value, timeout), r);
    }

    public CompletableFuture<Void> addq(byte[] key, int flags, int expires, byte[] value, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new AddQuietCommand(r, key, flags, expires, value, timeout), r);
    }

    public CompletableFuture<Version> replace(byte[] key,
                                              int flags,
                                              int expires,
                                              byte[] value,
                                              Version cas,
                                              Duration timeout) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new ReplaceCommand(r, key, flags, expires, value, cas, timeout), r);
    }


    public CompletableFuture<Void> replaceq(byte[] key,
                                            int flags,
                                            int expires,
                                            byte[] value,
                                            Version cas,
                                            Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new ReplaceQuietCommand(r, key, flags, expires, value, cas, timeout), r);
    }

    public CompletableFuture<Void> flush(int expires, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new FlushCommand(r, expires, timeout), r);
    }

    public CompletableFuture<Void> flushq(int expires, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new FlushQuietlyCommand(r, expires, timeout), r);
    }

    public CompletableFuture<Void> delete(byte[] key, Version cas, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new DeleteCommand(r, key, cas, timeout), r);
    }

    public CompletableFuture<Void> deleteq(byte[] key, Version cas, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new DeleteQuietlyCommand(r, key, cas, timeout), r);
    }

    public CompletableFuture<Counter> increment(byte[] key,
                                                long delta,
                                                long initial,
                                                int expiration,
                                                Version cas,
                                                Duration timeout) {
        CompletableFuture<Counter> r = new CompletableFuture<>();
        return enqueue(new IncrementCommand(r, key, delta, initial, expiration, cas, timeout), r);
    }

    public CompletableFuture<Void> incrementq(byte[] key,
                                              long delta,
                                              long initial,
                                              int expiration,
                                              Version cas,
                                              Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new IncrementQuietlyCommand(r, key, delta, initial, expiration, cas, timeout), r);
    }

    public CompletableFuture<Void> decrementq(byte[] key,
                                              long delta,
                                              long initial,
                                              int expiration,
                                              Version cas,
                                              Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new DecrementQuietlyCommand(r, key, delta, initial, expiration, cas, timeout), r);
    }

    public CompletableFuture<Counter> decrement(byte[] key,
                                                long delta,
                                                long initial,
                                                int expiration,
                                                Version cas,
                                                Duration timeout) {
        CompletableFuture<Counter> r = new CompletableFuture<>();
        return enqueue(new DecrementCommand(r, key, delta, initial, expiration, cas, timeout), r);
    }

    public CompletableFuture<Void> quit(Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new QuitCommand(r, this, timeout), r);
    }

    public CompletableFuture<Void> noop(Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new NoOpCommand(r, timeout), r);
    }

    public CompletableFuture<String> version(Duration timeout) {
        CompletableFuture<String> r = new CompletableFuture<>();
        return enqueue(new VersionCommand(r, timeout), r);
    }

    public CompletableFuture<Version> append(byte[] key, byte[] value, Version cas, Duration timeout) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new AppendCommand(r, key, value, cas, timeout), r);
    }

    public CompletableFuture<Void> appendq(byte[] key, byte[] value, Version cas, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new AppendQuietlyCommand(r, key, value, cas, timeout), r);
    }

    public CompletableFuture<Version> prepend(byte[] key, byte[] value, Version cas, Duration timeout) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new PrependCommand(r, key, value, cas, timeout), r);
    }

    public CompletableFuture<Void> prependq(byte[] key, byte[] value, Version cas, Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new PrependQuietlyCommand(r, key, value, cas, timeout), r);
    }

    public CompletableFuture<Map<String, String>> stat(Duration timeout) {
        CompletableFuture<Map<String, String>> r = new CompletableFuture<>();
        return enqueue(new StatCommand(r, Optional.empty(), timeout), r);
    }

    public CompletableFuture<Map<String, String>> stat(String key, Duration timeout) {
        CompletableFuture<Map<String, String>> r = new CompletableFuture<>();
        return enqueue(new StatCommand(r, Optional.of(key), timeout), r);
    }

    public CompletableFuture<Void> quitq(Duration timeout) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new QuitQuietlyCommand(r, timeout), r);
    }

    public void addNetworkFailureListener(Runnable callback) {
        networkFailureListeners.add(callback);
    }
}
