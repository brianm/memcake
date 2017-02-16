package org.skife.memcake;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectionTest {
    @Test
    public void testFoo() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2) ;
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(exec);
        Connection c = Connection.open(new InetSocketAddress("localhost", 11211), group).get();

        CompletableFuture<Version> sf = c.set("Hello".getBytes(StandardCharsets.UTF_8),
                                              0,
                                              0,
                                              "World".getBytes(StandardCharsets.UTF_8));

        Version cas = sf.get(2, TimeUnit.MINUTES);
        assertThat(cas).isNotNull();
        group.shutdown();

//        CompletableFuture<Value> gf = c.get("Hello".getBytes(StandardCharsets.UTF_8),
//                                            10,
//                                            TimeUnit.SECONDS);
//        Value v = gf.get();
    }
}
