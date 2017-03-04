package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

class StatCommand extends Command {

    private final CompletableFuture<Map<String, String>> result;
    private final Optional<String> key;

    StatCommand(CompletableFuture<Map<String, String>> result, Optional<String> key, Duration timeout) {
        super(timeout);
        this.result = result;
        this.key = key;
    }

    @Override
    Responder createResponder(int opaque) {
        final Map<String, String> rs = new ConcurrentHashMap<>();
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);
            byte[] key = r.getKey();
            if (key != null && key.length != 0) {
                // accumulate the value
                rs.put(new String(key, StandardCharsets.US_ASCII), new String(r.getValue(), StandardCharsets.US_ASCII));
            }
            else {
                // this is the last one!
                result.complete(rs);
            }
        });
    }

    @Override
    char keyLength() {
        return (char) key.orElse("").length();
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        key.ifPresent((k) -> {
            buffer.put(k.getBytes(StandardCharsets.UTF_8));
        });
    }

    @Override
    byte opcode() {
        return Opcodes.stat;
    }

    static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        // if there is a key, there will be a value. Parse them.
        if (response.getKeyLength() != 0) {
            byte[] key = new byte[response.getKeyLength()];
            bodyBuffer.get(key);
            response.setKey(key);

            byte[] value = new byte[response.getTotalBodyLength() - response.getKeyLength()];
            bodyBuffer.get(value);
            response.setValue(value);
        }
        conn.receive(response);
    }
}
