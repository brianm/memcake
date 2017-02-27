package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PrependQuietlyCommand extends AppendQuietlyCommand {
    PrependQuietlyCommand(CompletableFuture<Void> result,
                          byte[] key,
                          byte[] value,
                          Version cas,
                          long timeout,
                          TimeUnit timeoutUnit) {
        super(result, key, value, cas, timeout, timeoutUnit);
    }

    @Override
    byte opcode() {
        return Opcodes.prependq;
    }
}
