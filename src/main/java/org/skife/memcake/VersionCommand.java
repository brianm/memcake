package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class VersionCommand extends Command {

    private final CompletableFuture<String> result;

    VersionCommand(CompletableFuture<String> result, Duration timeout) {
        super(timeout);
        this.result = result;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);
            result.complete(new String(r.getValue(), StandardCharsets.US_ASCII));
        });
    }

    @Override
    byte opcode() {
        return Opcodes.version;
    }

    static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        byte[] value = new byte[response.getTotalBodyLength()];
        bodyBuffer.get(value);
        response.setValue(value);
        conn.receive(response);
    }
}
