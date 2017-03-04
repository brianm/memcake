package org.skife.memcake.connection;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class GetKQuietCommand extends GetCommand {
    GetKQuietCommand(CompletableFuture<Optional<Value>> result,
                     byte[] key,
                     Duration timeout
    ) {
        super(result, key, timeout);
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
