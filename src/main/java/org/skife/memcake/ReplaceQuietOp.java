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

import org.skife.memcake.connection.Version;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ReplaceQuietOp {
    private final Memcake memcake;
    private final byte[] key;
    private final byte[] value;

    private int flags = 0;
    private int expires = 0;
    private Duration timeout;
    private Version cas = Version.NONE;

    ReplaceQuietOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public ReplaceQuietOp flags(int flags) {
        this.flags = flags;
        return this;
    }

    public ReplaceQuietOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public ReplaceQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ReplaceQuietOp cas(Version cas) {
        this.cas = cas;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.replaceq(key, flags, expires, value, cas, timeout));
    }
}
