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

import org.skife.memcake.connection.Counter;
import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class DecrementOp {
    private final Memcake memcake;
    private final byte[] key;
    private final int delta;
    private Duration timeout;
    private int initialValue;
    private int expires = 0xFFFFFFFF;
    private Version cas = Version.NONE;

    DecrementOp(Memcake memcake, byte[] key, int delta, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.delta = delta;
        this.timeout = timeout;
    }

    public DecrementOp initialValue(int initial) {
        this.initialValue = initial;
        this.expires = 0;
        return this;
    }

    public DecrementOp cas(Version cas) {
        this.cas = cas;
        return this;
    }

    public DecrementOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public DecrementOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Counter> execute() {
        return memcake.call(key, (c) -> c.decrement(key, delta, initialValue, expires, cas, timeout));
    }
}
