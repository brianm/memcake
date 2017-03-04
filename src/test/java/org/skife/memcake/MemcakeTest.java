package org.skife.memcake;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.skife.memcake.connection.Connection;
import org.skife.memcake.connection.Value;
import org.skife.memcake.connection.Version;

import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

public class MemcakeTest {

    @ClassRule
    public static final MemcachedRule memcached = new MemcachedRule();

    private Connection c;
    private static ScheduledExecutorService cron = Executors.newScheduledThreadPool(1);

    @Before
    public void setUp() throws Exception {
        c = Connection.open(memcached.getAddress(),
                            AsynchronousSocketChannel.open(),
                            cron,
                            Duration.ofHours(1)).get();

        // yes yes, we use the thing under test to clean up after itself. It works though.
        c.flush(0).get();
    }

    @After
    public void tearDown() throws Exception {
        c.close();
    }


    @Test
    public void testApiDesignBytes() throws Exception {
        Memcake mc = Memcake.create(memcached.getAddress());
        CompletableFuture<Version> fs = mc.set(new byte[]{1, 2, 3}, new byte[]{3, 2, 1})
                                          .expires(0)
                                          .flags(1)
                                          .timeout(Duration.ofHours(1))
                                          .version(Version.NONE)
                                          .execute();
        Version ver = fs.get();

        Optional<Value> val = c.get(new byte[]{1, 2, 3}).get();
        assertThat(val).isPresent();
        val.ifPresent((v) -> {
            assertThat(v.getValue()).isEqualTo(new byte[]{3, 2, 1});
            assertThat(v.getFlags()).isEqualTo(1);
            assertThat(v.getVersion()).isEqualTo(ver);
        });
    }

    @Test
    public void testApiDesignStrings() throws Exception {
        Memcake mc = Memcake.create(memcached.getAddress());
        CompletableFuture<Version> fs = mc.set("hello", "world")
                                          .expires(0)
                                          .flags(1)
                                          .timeout(Duration.ofHours(1))
                                          .version(Version.NONE)
                                          .execute();
        Version ver = fs.get();

        Optional<Value> val = c.get("hello".getBytes(StandardCharsets.UTF_8)).get();
        assertThat(val).isPresent();
        val.ifPresent((v) -> {
            assertThat(v.getValue()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
            assertThat(v.getFlags()).isEqualTo(1);
            assertThat(v.getVersion()).isEqualTo(ver);
        });
    }

    @Test
    public void testApiDesignStringByte() throws Exception {
        Memcake mc = Memcake.create(memcached.getAddress());
        CompletableFuture<Version> fs = mc.set("hello", new byte[]{1, 2, 3})
                                          .expires(0)
                                          .flags(1)
                                          .timeout(Duration.ofHours(1))
                                          .version(Version.NONE)
                                          .execute();
        Version ver = fs.get();

        Optional<Value> val = c.get("hello".getBytes(StandardCharsets.UTF_8)).get();
        assertThat(val).isPresent();
        val.ifPresent((v) -> {
            assertThat(v.getValue()).isEqualTo(new byte[]{1, 2, 3});
            assertThat(v.getFlags()).isEqualTo(1);
            assertThat(v.getVersion()).isEqualTo(ver);
        });
    }
}
