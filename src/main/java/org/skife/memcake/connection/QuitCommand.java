package org.skife.memcake.connection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class QuitCommand extends Command {

    private final CompletableFuture<Void> result;
    private final Connection conn;

    QuitCommand(CompletableFuture<Void> result, Connection conn, Duration timeout) {
        super(timeout);
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
