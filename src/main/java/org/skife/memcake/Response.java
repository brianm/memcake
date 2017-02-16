package org.skife.memcake;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

class Response {
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
    private Integer flags;
    private byte[] value;
    private byte[] key;

    Response(Connection conn, ByteBuffer buf) {
        this.conn = conn;
        this.headerBuffer = buf;
        this.magic = buf.get();
        this.opcode = buf.get();
        this.keyLength = buf.getChar();
        this.extrasLength = buf.get();
        this.dataType = buf.get();
        this.status = buf.getChar();
        this.totalBodyLength = buf.getInt();
        this.opaque = buf.getInt();
        this.cas = buf.getLong();
        this.bodyBuffer = ByteBuffer.allocate(totalBodyLength);
    }

    void readBody() {
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

                // we finished reading body, so start next read loop
                headerBuffer.clear();
                conn.nextResponse(headerBuffer);
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                conn.networkFailure(exc);
            }
        });
    }

    private void readSetBody(ByteBuffer bodyBuffer) {
        conn.receive(this);
    }

    private void readGetBody(ByteBuffer bodyBuffer) {
        this.flags = bodyBuffer.getInt();
        if (this.keyLength != 0) {
            this.key = new byte[keyLength];
            bodyBuffer.get(key);
        }
        this.value = new byte[this.totalBodyLength - this.keyLength - this.extrasLength];
        bodyBuffer.get(this.value);
        conn.receive(this);
    }

    int getOpaque() {
        return opaque;
    }

    long getVersion() {
        return cas;
    }

    Integer getFlags() {
        return flags;
    }

    byte[] getValue() {
        return value;
    }
}
