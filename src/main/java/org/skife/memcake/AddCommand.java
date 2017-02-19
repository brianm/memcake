package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AddCommand extends SetCommand {
    AddCommand(CompletableFuture<Version> result,
               byte[] key,
               int flags,
               int expires,
               byte[] value,
               long timeout,
               TimeUnit unit) {
        super(result, key, flags, expires, value, timeout, unit);
    }

    @Override
    protected byte opCode() {
        return Opcodes.add;
    }
}
