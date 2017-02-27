package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PrependCommand extends AppendCommand {
    PrependCommand(CompletableFuture<Version> result,
                   byte[] key,
                   byte[] value,
                   Version cas,
                   long timeout,
                   TimeUnit unit) {
        super(result, key, value, cas, timeout, unit);
    }

    @Override
    byte opcode() {
        return Opcodes.prepend;
    }
}
