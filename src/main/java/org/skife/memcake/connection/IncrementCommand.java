/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.memcake.connection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class IncrementCommand extends Command {

    private final CompletableFuture<Counter> result;
    private final byte[] key;
    private final long delta;
    private final long initial;
    private final int expiration;
    private final Version cas;

    IncrementCommand(CompletableFuture<Counter> result,
                     byte[] key,
                     long delta,
                     long initial,
                     int expiration,
                     Version cas,
                     Duration timeout) {
        super(timeout);
        this.result = result;
        this.key = key;
        this.delta = delta;
        this.initial = initial;
        this.expiration = expiration;
        this.cas = cas;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);
            if (r.getStatus() == 0) {
                // found
                byte[] vbytes = r.getValue();
                if (vbytes.length != 8) {
                    result.completeExceptionally(new IllegalStateException("counter value was not a long (8 bytes!)"));
                    return;
                }

                Counter c = new Counter(ByteBuffer.wrap(vbytes).getLong(), new Version(r.getVersion()));
                result.complete(c);
            }
            else {
                result.completeExceptionally(new StatusException(r.getStatus(), r.getError()));
            }
        });
    }

    @Override
    byte extraLength() {
        return 20;
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.putLong(delta);
        buffer.putLong(initial);
        buffer.putInt(expiration);
        buffer.put(key);
    }

    @Override
    long cas() {
        return cas.token();
    }

    @Override
    byte opcode() {
        return Opcodes.increment;
    }

    public static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        byte[] value = new byte[response.getTotalBodyLength() - response.getKeyLength() - response.getExtrasLength()];
        bodyBuffer.get(value);
        response.setValue(value);
        conn.receive(response);
    }
}
