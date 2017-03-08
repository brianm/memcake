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

public class VersionOp {
    private final Memcake memcake;
    private Duration timeout;

    VersionOp(Memcake memcake, Duration timeout) {
        this.memcake = memcake;
        this.timeout = timeout;
    }

    public VersionOp timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public CompletableFuture<String> execute() {
        // TODO what to do about multiple servers?
        return memcake.call(null, (c) -> c.version(timeout));
    }
}
