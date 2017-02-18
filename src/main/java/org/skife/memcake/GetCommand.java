package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class GetCommand implements Command {

    private final CompletableFuture<Optional<Value>> result;
    private final byte[] key;

    GetCommand(CompletableFuture<Optional<Value>> result, byte[] key) {
        this.result = result;
        this.key = key;
    }

    @Override
    public Optional<Consumer<Map<Integer, Response>>> createConsumer(int opaque) {
        return Optional.of((s) -> {
            Response r = s.get(opaque);
            s.remove(opaque);

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
    public void write(Connection conn, Integer opaque) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + 8 + key.length);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(Opcodes.get);
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
