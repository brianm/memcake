/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.memcake;

import org.skife.memcake.connection.Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * High level client for memcached
 */
public class Memcake implements AutoCloseable {
    private static final ScheduledExecutorService cron = Executors.newScheduledThreadPool(2);

    private final LinkedBlockingQueue<Consumer<Connection>> reconnectQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<Connection> conn = new AtomicReference<>();
    private final AtomicReference<ClientState> state = new AtomicReference<>(ClientState.DISCONNECTED);
    private final Function<InetSocketAddress, CompletableFuture<Connection>> connector;
    private final InetSocketAddress addr;
    private final Duration timeout;

    private Memcake(Function<InetSocketAddress, CompletableFuture<Connection>> connector,
                    InetSocketAddress addr,
                    Duration timeout) {
        this.connector = connector;
        this.addr = addr;
        this.timeout = timeout;
        connect();
    }

    public static Memcake create(Set<InetSocketAddress> servers,
                                 Duration defaultTimeout,
                                 Function<InetSocketAddress, CompletableFuture<Connection>> connector) {
        if (servers.size() != 1) {
            throw new IllegalArgumentException("in this cas of memcake, only one server is supported. Sorry.");
        }
        return new Memcake(connector, servers.iterator().next(), defaultTimeout);
    }

    public static Memcake create(InetSocketAddress address,
                                 Duration defaultTimeout) {
        return create(Collections.singleton(address), defaultTimeout, (addr) -> {
            try {
                return Connection.open(addr, AsynchronousSocketChannel.open(), cron);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private void connect() {
        switch (state.get()) {
            case DISCONNECTED:
                if (state.compareAndSet(ClientState.DISCONNECTED, ClientState.CONNECTING)) {
                    // we won the connect race!
                    CompletableFuture<Connection> f = connector.apply(addr);
                    f.whenComplete((c, e) -> {
                        if (e != null) {
                            state.set(ClientState.DISCONNECTED);
                            connect();
                        }
                        else {
                            conn.set(c);
                            c.addNetworkFailureListener(() -> {
                                if (conn.compareAndSet(c, null)) {
                                    c.close();
                                    state.set(ClientState.DISCONNECTED);
                                    connect();
                                }
                            });
                            state.set(ClientState.CONNECTED);
                            for (Consumer<Connection> consumer : reconnectQueue) {
                                consumer.accept(c);
                            }
                        }
                    });
                }
                break;
            case CLOSED:
                throw new IllegalStateException("Memcake has been closed, it may no longer be used.");
            default:
                break;
        }
    }

    @Override
    public void close() throws Exception {
        switch (state.get()) {
            case CLOSED:
                break;
            case CONNECTED:
                conn.get().close();
                this.state.set(ClientState.DISCONNECTED);
                break;
            case CONNECTING:
                break;
            case DISCONNECTED:
                break;
        }
    }

    public <T> CompletableFuture<T> call(byte[] key, Function<Connection, CompletableFuture<T>> f) {
        switch (state.get()) {
            case CLOSED:
                throw new IllegalStateException("The memcake has been closed.");
            case CONNECTED:
                return f.apply(conn.get());
            case DISCONNECTED:
            case CONNECTING:
                CompletableFuture<T> nf = new CompletableFuture<T>();
                reconnectQueue.add((c) -> f.apply(c).whenComplete((r, e) -> {
                    if (e != null) {
                        nf.completeExceptionally(e);
                    }
                    else {
                        nf.complete(r);
                    }
                }));
                return nf;
            default:
                throw new IllegalStateException("unknown connection state: " + state);
        }
    }

    // public API

    public SetOp set(byte[] key, byte[] value) {
        return new SetOp(this, key, value, timeout);
    }

    public SetOp set(String key, String value) {
        return set(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public SetOp set(String key, byte[] value) {
        return set(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public GetOp get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    public GetOp get(byte[] key) {
        return new GetOp(this, key, timeout);
    }

    public GetWithKeyOp getk(byte[] key) {
        return new GetWithKeyOp(this, key, timeout);
    }

    public GetWithKeyOp getk(String key) {
        return getk(key.getBytes(StandardCharsets.UTF_8));
    }

    public AddOp add(byte[] key, byte[] value) {
        return new AddOp(this, key, value, timeout);
    }

    public AddOp add(String key, String value) {
        return add(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public AddOp add(String key, byte[] value) {
        return add(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public AddQuietOp addq(byte[] key, byte[] value) {
        return new AddQuietOp(this, key, value, timeout);
    }

    public AddQuietOp addq(String key, String value) {
        return addq(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public AddQuietOp addq(String key, byte[] value) {
        return addq(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public AppendOp append(byte[] key, byte[] value) {
        return new AppendOp(this, key, value, timeout);
    }

    public AppendOp append(String key, String value) {
        return append(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public AppendOp append(String key, byte[] value) {
        return append(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public AppendQuietOp appendq(byte[] key, byte[] value) {
        return new AppendQuietOp(this, key, value, timeout);
    }

    public AppendQuietOp appendq(String key, byte[] value) {
        return appendq(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public AppendQuietOp appendq(String key, String value) {
        return appendq(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public IncrementOp increment(byte[] key, int delta) {
        return new IncrementOp(this, key, delta, timeout);
    }

    public IncrementOp increment(String key, int delta) {
        return increment(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    public IncrementQuietOp incrementq(byte[] key, int delta) {
        return new IncrementQuietOp(this, key, delta, timeout);
    }

    public IncrementQuietOp incrementq(String key, int delta) {
        return incrementq(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    public DecrementOp decrement(byte[] key, int delta) {
        return new DecrementOp(this, key, delta, timeout);
    }

    public DecrementOp decrement(String key, int delta) {
        return decrement(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    public DecrementQuietOp decrementq(byte[] key, int delta) {
        return new DecrementQuietOp(this, key, delta, timeout);
    }

    public DecrementQuietOp decrementq(String key, int delta) {
        return decrementq(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    public DeleteOp delete(byte[] key) {
        return new DeleteOp(this, key, timeout);
    }

    public DeleteOp delete(String key) {
        return delete(key.getBytes(StandardCharsets.UTF_8));
    }

    public DeleteQuietOp deleteq(byte[] key) {
        return new DeleteQuietOp(this, key, timeout);
    }

    public DeleteQuietOp deleteq(String key) {
        return deleteq(key.getBytes(StandardCharsets.UTF_8));
    }

    public FlushOp flush() {
        return new FlushOp(this, timeout);
    }

    public FlushQuietOp flushq() {
        return new FlushQuietOp(this, timeout);
    }

    public GetWithKeyQuietOp getkq(byte[] key) {
        return new GetWithKeyQuietOp(this, key, timeout);
    }

    public GetWithKeyQuietOp getkq(String key) {
        return getkq(key.getBytes(StandardCharsets.UTF_8));
    }

    public GetQuietOp getq(byte[] key) {
        return new GetQuietOp(this, key, timeout);
    }

    public GetQuietOp getq(String key) {
        return getq(key.getBytes(StandardCharsets.UTF_8));
    }

    public NoOp noop() {
        return new NoOp(this, timeout);
    }

    public PrependOp prepend(byte[] key, byte[] value) {
        return new PrependOp(this, key, value, timeout);
    }

    public PrependOp prepend(String key, byte[] value) {
        return prepend(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public PrependOp prepend(String key, String value) {
        return prepend(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public PrependQuietOp prependq(byte[] key, byte[] value) {
        return new PrependQuietOp(this, key, value, timeout);
    }

    public PrependQuietOp prependq(String key, byte[] value) {
        return prependq(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public PrependQuietOp prependq(String key, String value) {
        return prependq(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public ReplaceOp replace(byte[] key, byte[] value) {
        return new ReplaceOp(this, key, value, timeout);
    }

    public ReplaceOp replace(String key, byte[] value) {
        return replace(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public ReplaceOp replace(String key, String value) {
        return replace(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public ReplaceQuietOp replaceq(byte[] key, byte[] value) {
        return new ReplaceQuietOp(this, key, value, timeout);
    }

    public ReplaceQuietOp replaceq(String key, byte[] value) {
        return replaceq(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public ReplaceQuietOp replaceq(String key, String value) {
        return replaceq(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public StatOp stat() {
        return new StatOp(this, timeout);
    }

    public VersionOp version() {
        return new VersionOp(this, timeout);
    }

    private enum ClientState {
        CONNECTED, CONNECTING, DISCONNECTED, CLOSED
    }
}
