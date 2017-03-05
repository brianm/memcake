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
package org.skife.memcake.connection;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class Responder {
    private final Consumer<Map<Integer, Response>> success;
    private final Consumer<Throwable> failure;
    private final int opaque;

    Responder(int opaque,
              Consumer<Throwable> failure,
              Consumer<Map<Integer, Response>> success) {
        this.success = success;
        this.failure = failure;
        this.opaque = opaque;
    }

    int completed(Map<Integer, Response> state) {
        success.accept(state);
        return opaque;
    }

    int failure(Throwable t) {
        failure.accept(t);
        return opaque;
    }

    static Responder voidResponder(Command c, CompletableFuture<Void> result, int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);

            if (c.isQuiet() && r == null) {
                // probably quiet, voidResponder can only handle Void quiets
                result.complete(null);
            }
            else if (r.getStatus() != 0) {
                result.completeExceptionally(new StatusException(r.getStatus(), r.getError()));
            }
            else {
                result.complete(null);
            }
        });
    }

    static Responder versionResponder(CompletableFuture<Version> result, int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);

            if (r.getStatus() == 0) {
                result.complete(new Version(r.getVersion()));
            }
            else {
                result.completeExceptionally(new StatusException(r.getStatus(),
                                                                 r.getError()));
            }
        });
    }
}
