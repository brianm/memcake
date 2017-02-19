package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Collections;
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
    public Optional<Responder> createResponder(int opaque) {
        return Optional.of(new Responder(Collections.singleton(opaque),
                                         result::completeExceptionally,
                                         (s) -> {
                                             Response r = s.get(opaque);
                                             s.remove(opaque);

                                             switch (r.getStatus()) {
                                                 case 0:
                                                     result.complete(Optional.of(new Value(new Version(r.getVersion()),
                                                                                           r.getFlags(),
                                                                                           r.getValue())));
                                                     break;
                                                 case 1:
                                                     result.complete(Optional.empty());
                                                     break;
                                                 default:
                                                     result.completeExceptionally(new StatusException(r.getStatus(),
                                                                                                      r.getError()));
                                             }
                                         }));
    }

    @Override
    public void write(Connection conn, Integer opaque) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + 8 + key.length);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(opcode());
        buffer.putChar((char) key.length);
        buffer.put((byte) 0x00); // extra length
        buffer.put((byte) 0x00); // data type
        buffer.putChar((char) 0x00); // vbucket
        buffer.putInt(key.length);
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        buffer.put(key);

        buffer.flip();
        Command.writeBuffer(conn, buffer);
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
