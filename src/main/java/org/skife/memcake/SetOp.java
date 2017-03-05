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

public class SetOp {
    protected final Memcake memcake;

    protected final byte[] key;
    protected final byte[] value;
    protected int expires = 0;
    protected int flags = 0;
    protected Version version = Version.NONE;
    protected Duration timeout;

    SetOp(Memcake memcake, byte[] key, byte[] value, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
    }

    public SetOp expires(int expires) {
        this.expires = expires;
        return this;
    }

    public SetOp flags(int flags) {
        this.flags = flags;
        return this;
    }

    public SetOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public SetOp cas(Version version) {
        this.version = version;
        return this;
    }

    public CompletableFuture<Version> execute() {
        return memcake.call(key, (c) -> c.set(key, flags, expires, value, version, timeout));
    }
}
