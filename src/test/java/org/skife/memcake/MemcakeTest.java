package org.skife.memcake;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import com.sun.xml.internal.ws.api.message.Packet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skife.memcake.connection.Connection;
import org.skife.memcake.connection.StatusException;
import org.skife.memcake.connection.Value;
import org.skife.memcake.connection.Version;
import org.skife.memcake.testing.Entry;
import org.skife.memcake.testing.MemcachedRule;

import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitQuickcheck.class)
public class MemcakeTest {

    private static final Duration TIMEOUT = Duration.ofHours(2);
    private static ScheduledExecutorService cron = Executors.newScheduledThreadPool(1);

    @ClassRule
    public static final MemcachedRule memcached = new MemcachedRule();

    private Connection c;
    private Memcake mc;

    @Before
    public void setUp() throws Exception {
        this.c = Connection.open(memcached.getAddress(),
                                 AsynchronousSocketChannel.open(),
                                 cron).get();
        c.flush(0, Duration.ofDays(1)).get();
        this.mc = Memcake.create(memcached.getAddress(), TIMEOUT);
    }

    @After
    public void tearDown() throws Exception {
        c.close();
        mc.close();
    }

    @Test
    public void testSetWithStrings() throws Exception {
        CompletableFuture<Version> fs = mc.set("hello", "world")
                                          .expires(0)
                                          .flags(1)
                                          .timeout(Duration.ofHours(1))
                                          .version(Version.NONE)
                                          .execute();
        Version ver = fs.get();

        Optional<Value> val = c.get("hello".getBytes(StandardCharsets.UTF_8), TIMEOUT).get();
        assertThat(val).isPresent();
        val.ifPresent((v) -> {
            assertThat(v.getValue()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
            assertThat(v.getFlags()).isEqualTo(1);
            assertThat(v.getVersion()).isEqualTo(ver);
        });
    }

    @Test
    public void testGetWithStrings() throws Exception {
        CompletableFuture<Version> fs = mc.set("hello", "world")
                                          .expires(0)
                                          .flags(1)
                                          .timeout(Duration.ofHours(1))
                                          .version(Version.NONE)
                                          .execute();
        Version ver = fs.get();

        CompletableFuture<Optional<Value>> rs = mc.get("hello".getBytes(StandardCharsets.UTF_8))
                                                  .timeout(Duration.ofHours(1))
                                                  .execute();
        Optional<Value> ov = rs.get();
        assertThat(ov).isPresent();
        ov.ifPresent((v) -> {
            assertThat(v.getVersion()).isEqualTo(ver);
            assertThat(v.getFlags()).isEqualTo(1);
            assertThat(v.getValue()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
        });
    }

    @Test
    public void testGetWithMappedResult() throws Exception {
        mc.set("hello", "world").execute().get();
        CompletableFuture<Optional<String>> rs = mc.get("hello")
                                                   .execute()
                                                   .thenApply(ov -> ov.map(v -> new String(v.getValue(),
                                                                                           StandardCharsets.UTF_8)));

        Optional<String> ov = rs.get();
        assertThat(ov).contains("world");
    }

    @Property
    public void checkGetKCorrect(Entry e) throws Exception {
        mc.set(e.key(), e.value()).execute().get();
        CompletableFuture<Optional<Value>> cf = mc.getk(e.key())
                                                  .timeout(TIMEOUT)
                                                  .execute();
        Optional<Value> ov = cf.get();
        assertThat(ov).isPresent();
        ov.ifPresent((v) -> {
            assertThat(v.getKey()).isPresent();
            v.getKey().ifPresent((k) -> {
                assertThat(k).isEqualTo(e.key());
            });
            assertThat(v.getValue()).isEqualTo(e.value());
        });
    }

    @Property
    public void checkAddSuccess(Entry e, int flags) throws ExecutionException, InterruptedException {
        CompletableFuture<Version> f = mc.add(e.key(), e.value())
                                         .flags(flags)
                                         .execute();
        Version vr = f.get();

        Value v = mc.get(e.key()).execute().get().get();
        assertThat(v.getValue()).isEqualTo(e.value());
        assertThat(v.getFlags()).isEqualTo(flags);
        assertThat(v.getVersion()).isEqualTo(vr);
    }

    @Property
    public void checkAddFailure(Entry e, int flags) throws ExecutionException, InterruptedException {
        mc.set(e.key(), e.value()).execute().get();

        CompletableFuture<Version> f = mc.add(e.key(), new byte[]{1, 2, 3})
                                         .flags(flags)
                                         .expires(8765)
                                         .execute();
        assertThatThrownBy(f::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void checkAddQuiet(Entry e) throws Exception {
        mc.addq(e.key(), e.value())
          .timeout(TIMEOUT)
          .execute();
        Value v = mc.get(e.key()).execute().get().get();
        assertThat(v.getValue()).isEqualTo(e.value());
    }

    @Property
    public void checkAddQuietFails(Entry e) throws Exception {
        mc.set(e.key(), e.value()).execute().get();

        CompletableFuture<Void> f = mc.addq(e.key(), new byte[]{1, 3, 3})
                                      .timeout(TIMEOUT)
                                      .execute();

        Value v = mc.get(e.key()).execute().get().get();

        assertThat(v.getValue()).isEqualTo(e.value());
        assertThatThrownBy(f::get).hasCauseInstanceOf(StatusException.class);
    }
}
