package org.skife.memcake;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DecrementQuietlyCommand extends IncrementQuietlyCommand {
    DecrementQuietlyCommand(CompletableFuture<Void> result,
                            byte[] key,
                            long delta,
                            long initial,
                            int expiration, long timeout, TimeUnit timeoutUnit) {
        super(result, key, delta, initial, expiration, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.decrementq;
    }
}
