package org.skife.memcake;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DeleteCommand extends Command {

    private final CompletableFuture<Void> result;
    private final byte[] key;

    protected DeleteCommand(CompletableFuture<Void> result, byte[] key, long timeout, TimeUnit unit) {
        super(timeout, unit);
        this.result = result;
        this.key = key;
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.standard(result, opaque);
    }

    @Override
    void write(Connection conn, Integer opaque) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + key.length);
        buffer.put(Bits.CLIENT_MAGIC);
        buffer.put(opcode());
        buffer.putChar((char)key.length); // key length
        buffer.put((byte) 0x00); // extra length
        buffer.put((byte) 0x00); // data type
        buffer.putChar((char) 0x00); // vbucket
        buffer.putInt(key.length); // totalBody
        buffer.putInt(opaque);
        buffer.putLong(0); //cas
        buffer.put(key);

        buffer.flip();
        Command.writeBuffer(conn, buffer);
    }

    @Override
    byte opcode() {
        return Opcodes.delete;
    }
}
