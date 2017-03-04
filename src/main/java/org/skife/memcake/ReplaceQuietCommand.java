package org.skife.memcake;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class ReplaceQuietCommand extends SetQuietCommand {
    ReplaceQuietCommand(CompletableFuture<Void> result,
                        byte[] key,
                        int flags,
                        int expires,
                        byte[] value,
                        Version cas,
                        Duration timeout) {
        super(result, key, flags, expires, value, cas, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.replaceq;
    }
}
