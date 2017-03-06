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

// TODO handle which one when we support multiple
//      possibly use an optional key to hash to, with null/default being "do to all"
public class NoOp {
    private final Memcake memcake;
    private Duration timeout;

    NoOp(Memcake memcake, Duration timeout) {
        this.memcake = memcake;
        this.timeout = timeout;
    }

    public NoOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<Void> execute() {
        return this.memcake.call(null, (c) -> c.noop(timeout));
    }
}
