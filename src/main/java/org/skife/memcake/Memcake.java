package org.skife.memcake;

import org.skife.memcake.connection.Connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
        conn.set(connector.apply(addr));
    }

    @Override
    public void close() throws Exception {
        CompletableFuture<Connection> c = conn.getAndSet(null);
        if (c != null) {
            c.thenAccept(Connection::close);
        }
    }

    public static Memcake create(InetSocketAddress... address) {
        if (address.length != 1) {
            throw new IllegalArgumentException("in this version of memcake, only one server is supported. Sorry.");
        }
        return new Memcake((a) -> {
            try {
                return Connection.open(a, AsynchronousSocketChannel.open(), cron);
            } catch (IOException e) {
                throw new IllegalStateException("unable to connect", e);
            }
        }, address[0], Duration.ofSeconds(1));
    }

    <T> CompletableFuture<T> perform(byte[] key, Function<Connection, CompletableFuture<T>> f) {
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
}
