package org.skife.memcake;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitQuickcheck.class)
public class ConnectionTest {

    private Connection c;

    @Before
    public void setUp() throws Exception {
        c = Connection.open(new InetSocketAddress("localhost", 11211),
                            AsynchronousSocketChannel.open()).get();
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

    @Property(trials = 10)
    @Ignore
    public void ifAddedCannotBeAddedAgain(@Size(min = 1, max = 64) List<Entry> entries) throws Exception {
        for (Entry entry : entries) {
            Version cas = c.add(entry.key(), 0, 0, entry.value()).get();
            Optional<Value> val = c.get(entry.key()).get();
            assertThat(val.get().getValue()).isEqualTo(entry.value());
            assertThat(val.get().getVersion()).isEqualTo(cas);
        }
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

}
