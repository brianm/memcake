package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PrependQuietlyCommand extends AppendQuietlyCommand {
    PrependQuietlyCommand(CompletableFuture<Void> result,
                          byte[] key,
                          byte[] value,
                          long timeout,
                          TimeUnit timeoutUnit) {
        super(result, key, value, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.prependq;
    }
}
