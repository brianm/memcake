package org.skife.memcake;

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
}
