package org.skife.memcake;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class GetQuietlyCommand extends GetCommand {

    GetQuietlyCommand(CompletableFuture<Optional<Value>> result, byte[] key, long timeout, TimeUnit unit) {
        super(result, key, timeout, unit);
    }

    @Override
    protected byte opcode() {
        return Opcodes.getq;
    }
}
