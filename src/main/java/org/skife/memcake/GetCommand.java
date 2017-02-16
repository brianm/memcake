package org.skife.memcake;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class GetCommand  {

    private final CompletableFuture<Value> result;
    private final byte[] key;
    private final long timeout;
    private final TimeUnit unit;

    public GetCommand(CompletableFuture<Value> result, byte[] key, long timeout, TimeUnit unit) {
        this.result = result;
        this.key = key;
        this.timeout = timeout;
        this.unit = unit;
    }


    public CompletableFuture<Value> execute(Connection conn) {
        final AsynchronousSocketChannel channel = conn.getChannel();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(Bits.CLIENT_MAGIC); // magic
            out.write(Opcodes.get); // opcode
            out.write(Bits.charToBytes(key.length)); // key length (2 bytes)

            out.write(0x00); // extras length
            out.write(0x00); // data type, must be 0x00 == raw bytes
            out.write(Bits.charToBytes(0)); // vbucket id (2 bytes)

            out.write(Bits.intToBytes(key.length)); // total body length

            out.write(0x00); // opaque byte 1
            out.write(0x00); // opaque byte 2
            out.write(0x00); // opaque byte 3
            out.write(0x00); // opaque byte 4

            out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // cas, 8 bytes, must be zeroed
            out.write(key);

        } catch (IOException e) {
            result.completeExceptionally(new IllegalStateException("unable to write to a ByteArrayOutputStream!", e));
        }


        channel.write(ByteBuffer.wrap(out.toByteArray()),
                      timeout,
                      unit,
                      null,
                      new CompletionHandler<Integer, Object>() {
                          @Override
                          public void completed(Integer bytesWritten, Object unused) {
                              ByteBuffer headerBuf = ByteBuffer.allocate(28);
                              awaitResponseHeader(conn, headerBuf);
                          }

                          @Override
                          public void failed(Throwable exc, Object unused) {
                              conn.close();
                              result.completeExceptionally(exc);
                          }
                      });

        return result;
    }

    private void awaitResponseHeader(Connection conn, ByteBuffer headerBuffer) {
        final AsynchronousSocketChannel channel = conn.getChannel();
        channel.read(headerBuffer, timeout, unit, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer bytesRead, Object attachment) {
                if (headerBuffer.remaining() != 0) {
                    awaitResponseHeader(conn, headerBuffer);
                    return;
                }

                if (bytesRead != 28) {
                    conn.close();
                    result.completeExceptionally(new IllegalStateException("bad response from server"));
                }

                headerBuffer.flip();
                if (headerBuffer.get() != Bits.SERVER_MAGIC) {
                    conn.close();
                    result.completeExceptionally(new IllegalStateException("bad response from server, wrong magic byte"));
                }

                if (headerBuffer.get() != Opcodes.get) {
                    conn.close();
                    result.completeExceptionally(new IllegalStateException("bad response from server, wrong opcode"));
                }

                headerBuffer.get(); // key length 1
                headerBuffer.get(); // key length 2

                if (headerBuffer.get() != 0x04) { // extra length -- should be 4
                    conn.close();
                    result.completeExceptionally(new IllegalStateException("bad response: expected 4 bytes of extras"));
                }

                headerBuffer.get(); // data type

                byte[] status = new byte[2];
                headerBuffer.get(status);

                byte[] bodyLengthBuffer = new byte[4];
                headerBuffer.get(bodyLengthBuffer);

                int valueLength = Bits.toUnsignedInt(bodyLengthBuffer) - 4 /* minus extras length*/;

                headerBuffer.get(); // opaque 1
                headerBuffer.get(); // opaque 2
                headerBuffer.get(); // opaque 3
                headerBuffer.get(); // opaque 4

                byte[] casBuffer = new byte[8];
                headerBuffer.get(casBuffer);

                byte[] flagBuffer = new byte[4];
                headerBuffer.get(flagBuffer);
                int flags = Bits.toUnsignedInt(flagBuffer);

                ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
                awaitValue(conn, new Version(1), flags, valueBuffer);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {

                result.completeExceptionally(exc);
            }
        });
    }


    private void awaitValue(Connection conn, Version cas, int flags, ByteBuffer buf) {
        conn.getChannel().read(buf, timeout, unit, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer bytesRead, Object attachment) {
                if (buf.remaining() != 0) {
                    // we need to read more bytes!
                    awaitValue(conn, cas, flags, buf);
                    return;
                }

                buf.flip();
                result.complete(new Value(cas, flags, buf.array()));
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                result.completeExceptionally(exc);
            }
        });
    }
}
