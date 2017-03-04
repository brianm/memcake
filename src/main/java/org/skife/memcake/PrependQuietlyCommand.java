package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class PrependQuietlyCommand extends AppendQuietlyCommand {
    PrependQuietlyCommand(CompletableFuture<Void> result,
                          byte[] key,
                          byte[] value,
                          Version cas,
                          Duration timeout) {
        super(result, key, value, cas, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.prependq;
    }
}
