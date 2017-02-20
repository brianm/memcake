package org.skife.memcake;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    // TODO put a limit on how deep this can grow!
    private final BlockingDeque<Command> queuedRequests = new LinkedBlockingDeque<>();

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

    /**
     * The main write "loop" used to ensure only one write is happening at a time.
     */
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
            }
            else {
                List<Integer> quiets = new ArrayList<>();
                queuedQuiets.drainTo(quiets);
                quietProxies.put(opaque, quiets);
            }

            c.write(this, opaque);
            timeoutExecutor.schedule(() -> {
                Responder r = waiting.remove(opaque);
                scoreboard.remove(opaque); // possible race between response coming in and timeout hitting
                r.failure(new TimeoutException());
            }, c.getTimeout(), c.getTimeoutUnit());
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
        close();
        waiting.forEach((_opaque, responder) -> responder.failure(exc));
        waiting.clear();
    }

    public boolean isOpen() {
        return open.get();
    }

    public void close() {
        if (open.compareAndSet(true, false)) {
            try {
                quit().get();
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

    AsynchronousSocketChannel getChannel() {
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
            waiting.remove(opaque);
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
        queuedRequests.add(command);
        maybeWrite();
        return result;
    }

    /* the main api of this thing, as used by users */

    public CompletableFuture<Optional<Value>> get(byte[] key) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetCommand(r, key, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Optional<Value>> getq(byte[] key) {
        CompletableFuture<Optional<Value>> r = new CompletableFuture<>();
        return enqueue(new GetQuietlyCommand(r, key, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Version> set(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new SetCommand(r, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Version> add(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new AddCommand(r, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Version> replace(byte[] key, int flags, int expires, byte[] value) {
        CompletableFuture<Version> r = new CompletableFuture<>();
        return enqueue(new ReplaceCommand(r, key, flags, expires, value, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Void> flush(int expires) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new FlushCommand(r, expires, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Void> delete(byte[] key) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new DeleteCommand(r, key, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Void> deleteq(byte[] key) {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new DeleteQuietlyCommand(r, key, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Counter> increment(byte[] key, long delta, long initial, int expiration) {
        CompletableFuture<Counter> r = new CompletableFuture<>();
        return enqueue(new IncrementCommand(r, key, delta, initial, expiration, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Counter> decrement(byte[] key, long delta, long initial, int expiration) {
        CompletableFuture<Counter> r = new CompletableFuture<>();
        return enqueue(new DecrementCommand(r, key, delta, initial, expiration, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Void> quit() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new QuitCommand(r, this, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<Void> noop() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        return enqueue(new NoOpCommand(r, defaultTimeout, defaultTimeoutUnit), r);
    }

    public CompletableFuture<String> version() {
        CompletableFuture<String> r = new CompletableFuture<>();
        return enqueue(new VersionCommand(r, defaultTimeout, defaultTimeoutUnit), r);
    }
}
