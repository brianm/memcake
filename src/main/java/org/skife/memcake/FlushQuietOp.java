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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

// TODO option to accept a key to shard to that key?
public class FlushQuietOp {
    private final Memcake memcake;
    private Duration timeout;
    private int expires = 0;

    FlushQuietOp(Memcake memcake, Duration timeout) {
        this.memcake = memcake;
        this.timeout = timeout;
    }

    public FlushQuietOp expires(int when) {
        this.expires = when;
        return this;
    }

    public FlushQuietOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Void> execute() {
        // TODO a way to indicate which one, or apply to all?
        return memcake.call(null, (c) -> c.flush(expires, timeout));
    }
}
