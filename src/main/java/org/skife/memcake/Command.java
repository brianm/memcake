package org.skife.memcake;

import java.util.Map;
import java.util.function.Consumer;

interface Command  {
    Consumer<Map<Integer, Response>> createConsumer(int opaque);
    void write(Connection conn, Integer opaque);
}
