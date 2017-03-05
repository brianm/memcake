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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * High level client for memcached
 */
public class Memcake implements AutoCloseable {
    private static final ScheduledExecutorService cron = Executors.newScheduledThreadPool(2);

    private final AtomicReference<CompletableFuture<Connection>> conn = new AtomicReference<>();

    private final Function<InetSocketAddress, CompletableFuture<Connection>> connector;
    private final InetSocketAddress addr;
    private final Duration defaultTimeout;

    private Memcake(Function<InetSocketAddress, CompletableFuture<Connection>> connector,
                    InetSocketAddress addr, Duration defaultTimeout) {
        this.connector = connector;
        this.addr = addr;
        this.defaultTimeout = defaultTimeout;
        connect();
    }

    private void connect() {
        Runnable reconnect = () -> {
            // jitter between 100ns to timeout, then try to connect
            long jitter = ThreadLocalRandom.current().nextLong(100, defaultTimeout.getNano());
            cron.schedule(this::connect, jitter, TimeUnit.NANOSECONDS);
        };

        // cleanly close the current connection
        CompletableFuture<Connection> current = conn.get();
        if (current != null) {
            current.whenComplete((c, e) -> {
                if (c != null) {
                    c.close();
                }
            });
        }

        final CompletableFuture<Connection> fc;
        try {
            fc = connector.apply(addr);
        } catch (RuntimeException e) {
            // connector is user supplied, so be careful in face of unexpected runtime exceptions :-)
            reconnect.run();
            return;
        }

        if (conn.compareAndSet(current, fc)) {
            // we set the current connection, wire up network failure listener
            fc.whenComplete((c, e) -> {
                if (e != null) {
                    reconnect.run();
                    return;
                }
                c.addNetworkFailureListener(this::connect);
            });
        }
        else {
            // we got into a connect race, we lost, kill this connection once it is up.
            fc.cancel(true);
            fc.whenComplete((c, e) -> {
                if (c != null) {
                    c.close();
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        CompletableFuture<Connection> c = conn.getAndSet(null);
        if (c != null) {
            c.thenAccept(Connection::close);
        }
    }

    public static Memcake create(Set<InetSocketAddress> servers,
                                 Duration defaultTimeout,
                                 Function<InetSocketAddress, CompletableFuture<Connection>> connector) {
        if (servers.size() != 1) {
            throw new IllegalArgumentException("in this version of memcake, only one server is supported. Sorry.");
        }
        return new Memcake(connector, servers.iterator().next(), defaultTimeout);
    }

    public static Memcake create(InetSocketAddress address, Duration defaultTimeout) {
        return create(Collections.singleton(address), defaultTimeout, (addr) -> {
            try {
                return Connection.open(addr, AsynchronousSocketChannel.open(), cron);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    public <T> CompletableFuture<T> call(byte[] key, Function<Connection, CompletableFuture<T>> f) {
        return conn.get().thenCompose(f);
    }

    // public API

    public SetOp set(byte[] key, byte[] value) {
        return new SetOp(this, key, value, defaultTimeout);
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
        return new GetOp(this, key, defaultTimeout);
    }

    public GetWithKeyOp getk(byte[] key) {
        return new GetWithKeyOp(this, key, defaultTimeout);
    }

    public GetWithKeyOp getk(String key) {
        return getk(key.getBytes(StandardCharsets.UTF_8));
    }
}
