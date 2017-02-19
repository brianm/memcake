package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DeleteQuietlyCommand extends DeleteCommand {
    protected DeleteQuietlyCommand(CompletableFuture<Void> result,
                                   byte[] key,
                                   long timeout,
                                   TimeUnit unit) {
        super(result, key, timeout, unit);
    }

    @Override
    public byte opcode() {
        return Opcodes.deleteq;
    }
}
