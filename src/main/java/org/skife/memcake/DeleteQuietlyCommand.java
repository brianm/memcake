package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DeleteQuietlyCommand extends DeleteCommand {
    DeleteQuietlyCommand(CompletableFuture<Void> result,
                         byte[] key,
                         Version version,
                         long timeout,
                         TimeUnit unit) {
        super(result, key, version, timeout, unit);
    }

    @Override
    public byte opcode() {
        return Opcodes.deleteq;
    }

    @Override
    boolean isQuiet() {
        return true;
    }
}
