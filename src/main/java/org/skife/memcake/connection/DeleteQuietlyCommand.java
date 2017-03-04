package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class DeleteQuietlyCommand extends DeleteCommand {
    DeleteQuietlyCommand(CompletableFuture<Void> result,
                         byte[] key,
                         Version version,
                         Duration timeout) {
        super(result, key, version, timeout);
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
