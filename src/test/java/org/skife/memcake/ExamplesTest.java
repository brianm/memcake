/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.memcake;

import org.junit.ClassRule;
import org.junit.Test;
import org.skife.memcake.connection.Connection;
import org.skife.memcake.connection.Value;
import org.skife.memcake.connection.Version;
import org.skife.memcake.testing.MemcachedRule;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.sun.javafx.applet.ExperimentalExtensions.get;
import static org.assertj.core.api.Assertions.assertThat;

public class ExamplesTest {
    @ClassRule
    public static final MemcachedRule memcached = new MemcachedRule();

    @Test
    public void createSimpleClient() throws Exception {
        Memcake mc = Memcake.create(memcached.getAddress(), // memcached address
                                    1000,                   // max concurrent requests
                                    Duration.ofSeconds(1)); // default operation timeout

        // use the mc client

        mc.close();
    }

    @Test
    public void createConfiguredNetworkyClient() throws Exception {
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(pool);

        Memcake mc = Memcake.create(Collections.singleton(memcached.getAddress()),
                                    Duration.ofSeconds(1),
                                    (addr) -> {
                                        try {
                                            AsynchronousSocketChannel chan = AsynchronousSocketChannel.open(group);
                                            return Connection.open(addr, 1000, chan, timer);
                                        } catch (IOException e) {
                                            throw new IllegalStateException(e);
                                        }
                                    });
        // use the mc client

        mc.close();
        timer.shutdown();
        pool.shutdown();
        group.shutdown();
    }

    @Test
    public void testGetAndSet() throws Exception {
        Memcake mc = Memcake.create(memcached.getAddress(), // memcached address
                                    1000,                   // max concurrent requests
                                    Duration.ofSeconds(1)); // default operation timeout

        CompletableFuture<Version> fv = mc.set("hello", "world")
                                          .expires(300)
                                          .flags(0xCAFE)
                                          .execute();

        Version v1 = fv.get();

        CompletableFuture<Optional<Value>> f = mc.get("hello")
                                                 .execute();

        Optional<Value> ov = f.get();

        assertThat(ov).isPresent();
        ov.ifPresent((v) -> {
            assertThat(v.getVersion()).isEqualTo(v1);
            assertThat(v.getFlags()).isEqualTo(0xCAFE);
            assertThat(v.getValue()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
        });

        mc.close();
    }
}
