package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class AddQuietCommand extends SetQuietCommand {

    AddQuietCommand(CompletableFuture<Void> result,
                    byte[] key,
                    int flags,
                    int expires,
                    byte[] value,
                    Duration timeout) {
        super(result, key, flags, expires, value, Version.ZERO, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.addq;
    }
}
