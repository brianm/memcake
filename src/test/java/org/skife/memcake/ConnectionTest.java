package org.skife.memcake;

import com.google.common.primitives.Bytes;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
    private static ScheduledExecutorService cron = Executors.newScheduledThreadPool(1);

    @Before
    public void setUp() throws Exception {
        c = Connection.open(mc.getAddress(),
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

    @Property
    public void setThenGetValue(Entry entry) throws Exception {
        CompletableFuture<Version> sf = c.set(entry.key(), 0, 0, entry.value());

        Version cas = sf.get(2, TimeUnit.SECONDS);
        assertThat(cas).isNotNull();

        CompletableFuture<Optional<Value>> gf = c.get(entry.key());
        Optional<Value> v = gf.get(2, TimeUnit.SECONDS);

        assertThat(v).isPresent();
        assertThat(v.get().getVersion()).isEqualTo(cas);
        assertThat(v.get().getValue()).isEqualTo(entry.value());
        assertThat(v.get().getKey()).isEmpty();
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
    public void flagsRoundTrip(Entry entry, int flags) throws Exception {
        c.set(entry.key(), flags, 0, entry.value()).get();
        Value v = c.get(entry.key()).get().get();
        assertThat(v.getFlags()).isEqualTo(flags);
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

    @Property
    public void setFailsWithIncorrectCas(Entry one, Entry two, Entry three) throws Exception {
        Version initial = c.set(one.key(), 0, 0, one.value()).get();
        c.set(one.key(), 0, 0, two.value());
        CompletableFuture<Version> shouldFail = c.set(one.key(), 0, 0, three.value(), initial);
        assertThatThrownBy(shouldFail::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void replaceFailsWithIncorrectCas(Entry one, Entry two, Entry three) throws Exception {
        Version initial = c.set(one.key(), 0, 0, one.value()).get();
        c.replace(one.key(), 0, 0, two.value());
        CompletableFuture<Version> shouldFail = c.replace(one.key(), 0, 0, three.value(), initial);
        assertThatThrownBy(shouldFail::get).hasCauseInstanceOf(StatusException.class);
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
        missingKey[0] = (byte) (missingKey[0] + 0x01);

        CompletableFuture<Optional<Value>> fq = c.getq(missingKey);
        Optional<Value> v = c.get(entry.key()).get();
        // request pipelined AFTER fq has completed, so we know fq is missing
        assertThat(fq.get()).isEmpty();
    }

    @Test
    public void testMultiGetBatchExample() throws Exception {
        c.set(new byte[]{1}, 0, 0, new byte[]{1}).get();
        c.set(new byte[]{3}, 0, 0, new byte[]{3}).get();
        c.set(new byte[]{5}, 0, 0, new byte[]{5}).get();

        // a protocol-level multiget, collecting results in a map.
        final Map<Integer, byte[]> results = new HashMap<>();
        c.getq(new byte[]{1}).thenAccept((o) -> o.ifPresent((v) -> results.put(1, v.getValue())));
        c.getq(new byte[]{2}).thenAccept((o) -> o.ifPresent((v) -> results.put(2, v.getValue())));
        c.getq(new byte[]{3}).thenAccept((o) -> o.ifPresent((v) -> results.put(3, v.getValue())));
        c.getq(new byte[]{4}).thenAccept((o) -> o.ifPresent((v) -> results.put(4, v.getValue())));
        c.get(new byte[]{5}).thenAccept((o) -> o.ifPresent((v) -> results.put(5, v.getValue())))
         .get();

        assertThat(results).containsOnlyKeys(1, 3, 5);
        assertThat(c.scoreboard).isEmpty();
        assertThat(c.waiting).isEmpty();
        assertThat(c.queuedQuiets).isEmpty();
        assertThat(c.quietProxies).isEmpty();
    }

    @Property
    public void delete(Entry e) throws Exception {
        c.set(e.key(), 0, 0, e.value()).get();
        c.delete(e.key()).get();
        assertThat(c.get(e.key()).get()).isEmpty();
    }

    @Property
    public void deleteWithCasSuccess(Entry one) throws Exception {
        Version v = c.set(one.key(), 0, 0, one.value()).get();
        c.delete(one.key(), v).get();
        assertThat(c.get(one.key()).get()).isEmpty();
    }

    @Property
    public void deleteWithCasFailure(Entry one, @From(ByteArrayGen.class) byte[] val2) throws Exception {
        Version v = c.set(one.key(), 0, 0, one.value()).get();
        c.replace(one.key(), 0, 0, val2);
        assertThatThrownBy(() -> c.delete(one.key(), v).get()).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void deleteQuietly(Entry e) throws Exception {
        c.set(e.key(), 0, 0, e.value()).get();
        c.deleteq(e.key());
        assertThat(c.get(e.key()).get()).isEmpty();
    }

    @Property
    public void deleteQuietlyWithCasSuccess(Entry e) throws Exception {
        Version v = c.set(e.key(), 0, 0, e.value()).get();
        c.deleteq(e.key(), v);
        assertThat(c.get(e.key()).get()).isEmpty();
    }

    @Property
    public void deleteQuietlyWithCasFailure(Entry one, @From(ByteArrayGen.class) byte[] val2) throws Exception {
        Version v = c.set(one.key(), 0, 0, one.value()).get();
        c.replace(one.key(), 0, 0, val2);
        CompletableFuture<Void> dq = c.deleteq(one.key(), v);
        c.noop().get();
        assertThatThrownBy(dq::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void increment(Entry entry,
                          @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long delta,
                          @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long initial) throws Exception {
        Counter one = c.increment(entry.key(), delta, initial, 0).get();
        assertThat(one.getValue()).isEqualTo(initial);
        Counter two = c.increment(entry.key(), delta, initial, 0).get();
        assertThat(two.getValue()).isEqualTo(initial + delta);
    }

    @Property
    public void decrement(Entry entry,
                          @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long delta,
                          @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long initial) throws Exception {
        Counter one = c.decrement(entry.key(), delta, initial, 0).get();
        assertThat(one.getValue()).isEqualTo(initial);
        Counter two = c.decrement(entry.key(), delta, initial, 0).get();
        assertThat(two.getValue()).isEqualTo(delta > initial ? 0 : initial - delta);
    }

    @Test
    public void testQuit() throws Exception {
        c.quit().get();
        assertThat(c.isOpen()).isFalse();
    }

    @Property
    public void testNoOpPushesQuietsThrough(Entry e) throws Exception {
        CompletableFuture<Optional<Value>> set = c.getq(e.key());
        CompletableFuture<Void> noop = c.noop();
        noop.get();
        assertThat(set.get(1, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @Test
    public void testVersion() throws Exception {
        String version = c.version().get();
        assertThat(version).isNotNull().isNotEmpty();
    }

    @Property
    public void getkIncludesKeyOnValue(Entry e) throws Exception {
        c.set(e.key(), 0, 0, e.value()).get();
        Value v = c.getk(e.key()).get().get();
        assertThat(v.getKey()).isPresent();
        assertThat(v.getKey().get()).isEqualTo(e.key());
    }

    @Property
    public void testGetKeyQuietly(Entry e) throws Exception {
        c.set(e.key(), 0, 0, e.value()).get();
        CompletableFuture<Optional<Value>> one = c.getkq(e.key());
        c.noop().get();
        Optional<Value> v = one.get();
        assertThat(v).isPresent();
    }

    @Property
    public void checkAppendAppends(Entry entry) throws Exception {
        c.set(entry.key(), 0, 0, new byte[]{0});
        c.append(entry.key(), new byte[]{1});
        Value v = c.get(entry.key()).get().get();
        assertThat(v.getValue()).isEqualTo(new byte[]{0, 1});
    }

    @Property
    public void checkPrependPrepends(Entry entry) throws Exception {
        c.set(entry.key(), 0, 0, new byte[]{0});
        c.prepend(entry.key(), new byte[]{1});
        Value v = c.get(entry.key()).get().get();
        assertThat(v.getValue()).isEqualTo(new byte[]{1, 0});
    }

    @Test
    public void testStat() throws Exception {
        Map<String, String> stats = c.stat().get();
        assertThat(stats).containsKey("pid")
                         .containsKey("total_items");
    }

    @Test
    public void testStatOnlyOne() throws Exception {
        c.set("a".getBytes(), 0, 0, new byte[]{1});
        c.set("b".getBytes(), 0, 0, new byte[]{1});
        c.set("c".getBytes(), 0, 0, new byte[]{1});

        Map<String, String> stats = c.stat("items").get();
        assertThat(stats).containsEntry("items:1:number", "3");
    }

    @Property
    public void checkSetQSetsWithoutCas(Entry entry) throws Exception {
        c.setq(entry.key(), 0, 0, entry.value());
        Value v = c.get(entry.key()).get().get();
        assertThat(v.getValue()).isEqualTo(entry.value());
    }

    @Property
    public void checkSetQSetsWithCasSuccess(Entry one, Entry two) throws Exception {
        Version cas = c.set(one.key(), 0, 0, one.value()).get();
        c.setq(one.key(), 0, 0, two.value(), cas);
        Value v = c.get(one.key()).get().get();
        assertThat(v.getValue()).isEqualTo(two.value());
    }

    @Property
    public void checkSetQSetsWithCasFail(Entry one, Entry two, Entry three) throws Exception {
        Version cas1 = c.set(one.key(), 0, 0, one.value()).get();
        c.set(one.key(), 0, 0, two.value()).get();
        CompletableFuture<Void> shouldFail = c.setq(one.key(), 0, 0, three.value(), cas1);
        Value v = c.get(one.key()).get().get();
        assertThat(v.getValue()).isEqualTo(two.value());
        assertThatThrownBy(shouldFail::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void checkAddQuietlyAddsIfMissing(Entry entry) throws Exception {
        c.addq(entry.key(), 0, 0, entry.value());
        assertThat(c.get(entry.key()).get().get().getValue()).isEqualTo(entry.value());
    }

    @Property
    public void checkAddQuietlyDoesNotAddIfPressent(Entry existing, Entry entry) throws Exception {
        c.addq(existing.key(), 0, 0, existing.value());
        CompletableFuture<Void> rs = c.addq(existing.key(), 0, 0, entry.value());
        assertThat(c.get(existing.key()).get().get().getValue()).isEqualTo(existing.value());
        assertThatThrownBy(rs::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void checkReplaceReplaces(Entry one, Entry two) throws Exception {
        c.addq(one.key(), 0, 0, one.value());
        CompletableFuture<Void> rs = c.replaceq(one.key(), 0, 0, two.value());
        Value v = c.get(one.key()).get().get();

        assertThat(v.getValue()).isEqualTo(two.value());
        rs.get();
        assertThat(rs.isDone()).isTrue();
    }

    @Property
    public void checkReplaceDoesNotReplaceIfNotThere(Entry one) throws Exception {
        CompletableFuture<Void> rs = c.replaceq(one.key(), 0, 0, one.value());
        c.noop().get();
        assertThatThrownBy(rs::get).hasCauseInstanceOf(StatusException.class);
    }

    @Property
    public void checkIncrementQuietlyIncrements(Entry entry,
                                                @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long delta,
                                                @InRange(minLong = 1, maxLong = Integer.MAX_VALUE) long initial) throws Exception {
        c.incrementq(entry.key(), 0, initial, 0);
        Counter cnt = c.increment(entry.key(), 0, 0, 0).get();
        assertThat(cnt.getValue()).isEqualTo(initial);

        c.incrementq(entry.key(), delta, 0, 0);
        cnt = c.increment(entry.key(), 0, 0, 0).get();
        assertThat(cnt.getValue()).isEqualTo(initial + delta);
    }

    @Property
    public void checkDecrementQuietlyDecrements(Entry entry,
                                                @InRange(minLong = 1, maxLong = 500) long delta,
                                                @InRange(minLong = 1000, maxLong = 20000) long initial) throws Exception {
        c.decrementq(entry.key(), 0, initial, 0);
        Counter cnt = c.increment(entry.key(), 0, 0, 0).get();
        assertThat(cnt.getValue()).isEqualTo(initial);

        c.decrementq(entry.key(), delta, 0, 0);
        cnt = c.increment(entry.key(), 0, 0, 0).get();
        assertThat(cnt.getValue()).isEqualTo(initial - delta);
    }

    @Test
    public void testQuitQuietly() throws Exception {
        c.quitq();
        CompletableFuture<Void> noop = c.noop();
        assertThatThrownBy(noop::get).hasCauseInstanceOf(IOException.class);
    }

    @Test
    public void testFlushQ() throws Exception {
        c.set(new byte[]{1}, 0, 0, new byte[]{1});
        c.flushq(0);
        Optional<Value> v = c.get(new byte[]{1}).get();
        assertThat(v).isEmpty();
    }

    @Property
    public void checkAppendQuietly(@From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] key,
                                   @From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] initial,
                                   @From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] additional) throws Exception {
        c.set(key, 0, 0, initial);
        c.appendq(key, additional);
        Value v = c.get(key).get().get();

        assertThat(v.getValue()).isEqualTo(Bytes.concat(initial, additional));
    }

    @Property
    public void checkPrependQuietly(@From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] key,
                                    @From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] initial,
                                    @From(ByteArrayGen.class) @Size(min = 1, max = 100) byte[] additional) throws Exception {
        c.set(key, 0, 0, initial);
        c.prependq(key, additional);
        Value v = c.get(key).get().get();

        assertThat(v.getValue()).isEqualTo(Bytes.concat(additional, initial));
    }
}
