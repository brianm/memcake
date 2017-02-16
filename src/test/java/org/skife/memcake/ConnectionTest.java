package org.skife.memcake;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitQuickcheck.class)
public class ConnectionTest {

    private Connection c;

    @Before
    public void setUp() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2);
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(exec);
        c = Connection.open(new InetSocketAddress("localhost", 11211), group).get();
    }

    @Property
    public void setThenGetValue(Entry entry) throws Exception {
        CompletableFuture<Version> sf = c.set(entry.key(), 0, 0, entry.value());

        Version cas = sf.get(2, TimeUnit.SECONDS);
        assertThat(cas).isNotNull();

        CompletableFuture<Value> gf = c.get(entry.key());
        Value v = gf.get(2, TimeUnit.SECONDS);

        assertThat(v.getCas()).isEqualTo(cas);
        assertThat(v.getValue()).isEqualTo(entry.value());
    }

    @Property(trials = 10)
    public void pipelinedSetsAndGets(@Size(min=1, max = 64) List<Entry> entries) throws Exception {
        List<CompletableFuture<Version>> futureSets= new ArrayList<>();
        for (Entry entry : entries) {
            futureSets.add(c.set(entry.key(), 0, 0, entry.value()));
        }

        List<CompletableFuture<Value>> futureValues = new ArrayList<>();
        for (Entry entry : entries) {
            futureValues.add(c.get(entry.key()));
        }

        for (CompletableFuture<Version> f : futureSets) {
            f.get(10, TimeUnit.SECONDS);
        }

        for (int i = 0; i < entries.size(); i++) {
            Value v= futureValues.get(i).get();
            assertThat(v.getValue()).isEqualTo(entries.get(i).value());
        }
    }

}
