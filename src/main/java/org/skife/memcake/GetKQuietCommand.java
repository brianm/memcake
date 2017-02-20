package org.skife.memcake;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class GetKQuietCommand extends GetCommand {
    GetKQuietCommand(CompletableFuture<Optional<Value>> result,
                     byte[] key,
                     long timeout,
                     TimeUnit unit) {
        super(result, key, timeout, unit);
    }

    @Override
    boolean isQuiet() {
        return true;
    }

    @Override
    protected byte opcode() {
        return Opcodes.getkq;
    }
}
