package org.skife.memcake;

import java.util.Map;
import java.util.function.Consumer;

class Responder {
    private final Consumer<Map<Integer, Response>> success;
    private final Consumer<Throwable> failure;

    Responder(Consumer<Map<Integer, Response>> success,
              Consumer<Throwable> failure) {
        this.success = success;
        this.failure = failure;
    }

    void success(Map<Integer, Response> state) {
        success.accept(state);
    }

    void failure(Throwable t) {
        failure.accept(t);
    }
}
