package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public class Response {
    private final Connection conn;
    private final ByteBuffer headerBuffer;

    private final byte magic;
    private final byte opcode;
    private final char keyLength;
    private final byte extrasLength;
    private final byte dataType;
    private final int totalBodyLength;
    private final int opaque;
    private final long cas;
    private final ByteBuffer bodyBuffer;
    private final char status;

    public Response(Connection conn, ByteBuffer buf) {
        this.conn = conn;
        headerBuffer = buf;
        magic = buf.get();
        opcode = buf.get();
        keyLength = buf.getChar();
        extrasLength = buf.get();
        dataType = buf.get();
        status = buf.getChar();
        totalBodyLength = buf.getInt();
        opaque = buf.getInt();
        cas = buf.getLong();
        bodyBuffer = ByteBuffer.allocate(totalBodyLength);
        headerBuffer.clear();
    }

    public void readBody() {
        conn.getChannel().read(bodyBuffer, bodyBuffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer bytesRead, ByteBuffer bodyBuffer) {
                if (bodyBuffer.remaining() != 0) {
                    readBody();
                    return;
                }

                bodyBuffer.flip();
                switch (opcode) {
                    case Opcodes.set:
                        readSetBody(bodyBuffer);
                        break;
                    case Opcodes.get:
                        readGetBody(bodyBuffer);
                        break;
                    default:
                        System.err.printf("unhandled opcode: %d\n", opcode);

                }
                finished();
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                conn.networkFailure(exc);
            }
        });
    }

    private void finished() {
        conn.nextResponse(headerBuffer);
    }

    private void readSetBody(ByteBuffer bodyBuffer) {
        conn.receive(this);
    }

    private void readGetBody(ByteBuffer bodyBuffer) {

    }

    public int getOpaque() {
        return opaque;
    }

    public long getVersion() {
        return cas;
    }
}
