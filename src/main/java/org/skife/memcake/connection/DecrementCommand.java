package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class DecrementCommand extends IncrementCommand {

    DecrementCommand(CompletableFuture<Counter> result,
                     byte[] key,
                     long delta,
                     long initial,
                     int expiration,
                     Version cas,
                     Duration timeout) {
        super(result, key, delta, initial, expiration, cas, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.decrement;
    }

}
