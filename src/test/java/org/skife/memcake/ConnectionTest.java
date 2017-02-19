package org.skife.memcake;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitQuickcheck.class)
public class ConnectionTest {

    @ClassRule
    public static final MemcachedRule mc = new MemcachedRule();

    private Connection c;
    private ScheduledExecutorService cron;

    @Before
    public void setUp() throws Exception {
        cron = Executors.newScheduledThreadPool(1);
        c = Connection.open(mc.getAddress(),
                            AsynchronousSocketChannel.open(),
                            cron, 1, TimeUnit.SECONDS).get();

        // yes yes, we use the thing under test to clean up after itself. It works though.
        c.flush(0).get();
    }

    @After
    public void tearDown() throws Exception {
        cron.shutdown();
        c.close();
    }

    @Property
    public void setThenGetValue(Entry entry) throws Exception {
        CompletableFuture<Version> sf = c.set(entry.key(), 0, 0, entry.value());

        Version cas = sf.get(2, TimeUnit.SECONDS);
        assertThat(cas).isNotNull();

        CompletableFuture<Optional<Value>> gf = c.get(entry.key());
        Optional<Value> v = gf.get(2, TimeUnit.SECONDS);

        assertThat(v.get().getVersion()).isEqualTo(cas);
        assertThat(v.get().getValue()).isEqualTo(entry.value());
    }

    @Property(trials = 10)
    public void pipelinedSetsAndGets(@Size(min = 1, max = 64) List<Entry> entries) throws Exception {
        List<CompletableFuture<Version>> futureSets = new ArrayList<>();
        for (Entry entry : entries) {
            futureSets.add(c.set(entry.key(), 0, 0, entry.value()));
        }

        List<CompletableFuture<Optional<Value>>> futureValues = new ArrayList<>();
        for (Entry entry : entries) {
            futureValues.add(c.get(entry.key()));
        }

        for (CompletableFuture<Version> f : futureSets) {
            f.get(10, TimeUnit.SECONDS);
        }

        for (int i = 0; i < entries.size(); i++) {
            Optional<Value> v = futureValues.get(i).get();
            assertThat(v.get().getValue()).isEqualTo(entries.get(i).value());
        }
    }

    @Property
    public void ifAddedCannotBeAddedAgain(Entry entry) throws Exception {
        Version cas = c.add(entry.key(), 0, 0, entry.value()).get();
        Value val = c.get(entry.key()).get().get();

        assertThat(val.getValue()).isEqualTo(entry.value());
        assertThat(val.getVersion()).isEqualTo(cas);

        CompletableFuture<Version> again = c.add(entry.key(), 0, 0, new byte[]{0x00, 0x01});

        assertThatThrownBy(again::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void replaceMustExistToBeReplaced(Entry entry) throws Exception {
        c.add(entry.key(), 0, 0, new byte[]{0x01}).get();
        c.replace(entry.key(), 0, 0, entry.value()).get();

        c.flush(0).get();

        assertThatThrownBy(() -> c.replace(entry.key(), 0, 0, entry.value()).get())
                .hasCauseInstanceOf(StatusException.class);
    }

    @Test
    public void testFlush() throws Exception {
        c.set("hello".getBytes(StandardCharsets.UTF_8),
              0,
              0,
              "world".getBytes(StandardCharsets.UTF_8))
         .get();
        c.flush(0).get();

        Optional<Value> missing = c.get("hello".getBytes(StandardCharsets.UTF_8)).get();
        assertThat(missing).isEmpty();
    }

    @Property
    public void testGetQuietly(Entry entry) throws Exception {
        c.set(entry.key(), 0, 0, entry.value()).get();
        byte[] missingKey = entry.key();
        missingKey[0] = (byte)(missingKey[0] + 0x01);

        CompletableFuture<Optional<Value>> fq =  c.getq(missingKey);
        Optional<Value> v = c.get(entry.key()).get();
        // request pipelined AFTER fq has completed, so we know fq is missing
        assertThat(fq.get()).isEmpty();
    }
}
