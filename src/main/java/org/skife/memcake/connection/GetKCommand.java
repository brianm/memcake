package org.skife.memcake.connection;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class GetKCommand extends GetCommand {
    GetKCommand(CompletableFuture<Optional<Value>> result,
                byte[] key,
                Duration timeout) {
        super(result, key, timeout);
    }

    @Override
    protected byte opcode() {
        return Opcodes.getk;
    }
}
