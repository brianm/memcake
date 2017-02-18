package org.skife.memcake;

import java.util.concurrent.CompletableFuture;

class ReplaceCommand extends SetCommand {
    ReplaceCommand(CompletableFuture<Version> result, byte[] key, int flags, int expires, byte[] value) {
        super(result, key, flags, expires, value);
    }

    @Override
    protected byte opCode() {
        return Opcodes.replace;
    }
}
