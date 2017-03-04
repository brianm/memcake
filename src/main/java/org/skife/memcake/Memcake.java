package org.skife.memcake;

import org.skife.memcake.connection.Connection;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * High level client for memcached
 */
public class Memcake implements AutoCloseable {
    private final Supplier<Connection> connector;
    private final AtomicReference<Connection> conn = new AtomicReference<>();

    private Memcake(Supplier<Connection> connector) {
        this.connector = connector;
        conn.set(connector.get());
    }


    @Override
    public void close() throws Exception {

    }
}
