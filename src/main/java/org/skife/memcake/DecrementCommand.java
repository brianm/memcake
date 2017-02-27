package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DecrementCommand extends IncrementCommand {

    DecrementCommand(CompletableFuture<Counter> result,
                     byte[] key,
                     long delta,
                     long initial,
                     int expiration,
                     Version cas,
                     long timeout, TimeUnit unit) {
        super(result, key, delta, initial, expiration, cas, timeout, unit);
    }

    @Override
    byte opcode() {
        return Opcodes.decrement;
    }

}
