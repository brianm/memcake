package org.skife.memcake;

import java.util.concurrent.CompletableFuture;

public class AddCommand extends SetCommand {
    AddCommand(CompletableFuture<Version> result, byte[] key, int flags, int expires, byte[] value) {
        super(result, key, flags, expires, value);
    }

    @Override
    protected byte opCode() {
        return Opcodes.add;
    }
}
