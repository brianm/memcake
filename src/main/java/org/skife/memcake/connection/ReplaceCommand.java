package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class ReplaceCommand extends SetCommand {

    ReplaceCommand(CompletableFuture<Version> result,
                   byte[] key,
                   int flags,
                   int expires,
                   byte[] value,
                   Version casToken,
                   Duration timeout) {
        super(result, key, flags, expires, value, casToken, timeout);
    }

    @Override
    protected byte opcode() {
        return Opcodes.replace;
    }
}
