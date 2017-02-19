package org.skife.memcake;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

class Responder {
    private final Consumer<Map<Integer, Response>> success;
    private final Consumer<Throwable> failure;
    private final Collection<Integer> opaques;

    Responder(Collection<Integer> opaques,
              Consumer<Throwable> failure,
              Consumer<Map<Integer, Response>> success) {
        this.success = success;
        this.failure = failure;
        this.opaques = opaques;
    }

    Collection<Integer> success(Map<Integer, Response> state) {
        success.accept(state);
        return opaques;
    }

    Collection<Integer> failure(Throwable t) {
        failure.accept(t);
        return opaques;
    }
}
