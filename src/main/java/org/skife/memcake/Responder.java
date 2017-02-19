package org.skife.memcake;

import java.util.Map;
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
}
