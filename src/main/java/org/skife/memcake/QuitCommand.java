package org.skife.memcake;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class QuitCommand extends Command {

    private final CompletableFuture<Void> result;
    private final Connection conn;

    QuitCommand(CompletableFuture<Void> result, Connection conn, long timeout, TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.conn = conn;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            result.complete(null);
            conn.close();
        });
    }

    @Override
    byte opcode() {
        return Opcodes.quit;
    }
}
