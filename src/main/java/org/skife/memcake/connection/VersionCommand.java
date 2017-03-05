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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class VersionCommand extends Command {

    private final CompletableFuture<String> result;

    VersionCommand(CompletableFuture<String> result, Duration timeout) {
        super(timeout);
        this.result = result;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            Response r = s.get(opaque);
            result.complete(new String(r.getValue(), StandardCharsets.US_ASCII));
        });
    }

    @Override
    byte opcode() {
        return Opcodes.version;
    }

    static void parseBody(Response response, Connection conn, ByteBuffer bodyBuffer) {
        byte[] value = new byte[response.getTotalBodyLength()];
        bodyBuffer.get(value);
        response.setValue(value);
        conn.receive(response);
    }
}
