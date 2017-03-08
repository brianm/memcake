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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class StatOp {
    private final Memcake memcake;
    private final Duration timeout;
    private Optional<String> type = Optional.empty();

    StatOp(Memcake memcake, Duration timeout) {
        this.memcake = memcake;
        this.timeout = timeout;
    }

    public StatOp type(String type) {
        this.type = Optional.ofNullable(type);
        return this;
    }

    public CompletableFuture<Map<String, String>> execute() {
        // TODO -- what to do about multiple servers?
        return memcake.call(null, (c) -> {
            if (type.isPresent()) {
                return c.stat(type.get(), timeout);
            } else {
                return c.stat(timeout);
            }
        });
    }
}
