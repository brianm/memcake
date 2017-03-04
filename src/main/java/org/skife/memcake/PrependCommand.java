package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class PrependCommand extends AppendCommand {
    PrependCommand(CompletableFuture<Version> result,
                   byte[] key,
                   byte[] value,
                   Version cas,
                   Duration timeout) {
        super(result, key, value, cas, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.prepend;
    }
}
