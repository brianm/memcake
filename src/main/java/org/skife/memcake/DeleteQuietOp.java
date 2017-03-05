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

public class DeleteQuietOp {
    private final Memcake memcake;
    private final byte[] key;

    private Duration timeout;
    private Version cas = Version.NONE;

    DeleteQuietOp(Memcake memcake, byte[] key, Duration timeout) {
        this.memcake = memcake;
        this.key = key;
        this.timeout = timeout;
    }

    public DeleteQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public DeleteQuietOp cas(Version version) {
        this.cas = version;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return memcake.call(key, (c) -> c.deleteq(key, cas, timeout));
    }
}
