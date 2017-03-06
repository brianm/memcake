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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class GetCommand extends Command {

    private final CompletableFuture<Optional<Value>> result;
    private final byte[] key;

    GetCommand(CompletableFuture<Optional<Value>> result, byte[] key, Duration timeout) {
        super(timeout);
        this.result = result;
        this.key = key;
    }

    @Override
    public Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);

            if ((r == null) && (opcode() == Opcodes.getq || opcode() == Opcodes.getkq)) {
                // this was a getq and we didn't get a response, but
                // a future nonquiet query led us here.
                result.complete(Optional.empty());
                return;
            }

            switch (r.getStatus()) {
                case 0:
                    result.complete(Optional.of(new Value(new Version(r.getVersion()),
                                                          r.getFlags(),
                                                          Optional.ofNullable(r.getKey()),
                                                          r.getValue())));
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
