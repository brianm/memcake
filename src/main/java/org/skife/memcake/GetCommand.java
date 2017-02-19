package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class GetCommand extends Command {

    private final CompletableFuture<Optional<Value>> result;
    private final byte[] key;

    GetCommand(CompletableFuture<Optional<Value>> result, byte[] key, long timeout, TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
    }

    @Override
    public Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);

            if (r == null && opcode() == Opcodes.getq) {
                // this was a getq and we didn't get a response, but
                // a future nonquiet query led us here.
                result.complete(Optional.empty());
                return;
            }

            switch (r.getStatus()) {
                case 0:
                    result.complete(Optional.of(new Value(new Version(r.getVersion()), r.getFlags(), r.getValue())));
                    break;
                case 1:
                    result.complete(Optional.empty());
                    break;
                default:
                    result.completeExceptionally(new StatusException(r.getStatus(), r.getError()));
            }
        });
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.put(key);
    }

    protected byte opcode() {
        return Opcodes.get;
    }

    static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        response.setFlags(bodyBuffer.getInt());
        if (response.getKeyLength() != 0) {
            byte[] key = new byte[response.getKeyLength()];
            bodyBuffer.get(key);
            response.setKey(key);
        }
        byte[] value = new byte[response.getTotalBodyLength() - response.getKeyLength() - response.getExtrasLength()];
        bodyBuffer.get(value);
        response.setValue(value);
        conn.receive(response);
    }
}
