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

class SetCommand extends Command {
    private final CompletableFuture<Version> result;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;
    private final Version casToken;

    SetCommand(CompletableFuture<Version> result,
               byte[] key,
               int flags,
               int expires,
               byte[] value,
               Version casToken,
               Duration timeout) {
        super(timeout);
        this.result = result;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
        this.casToken = casToken;
    }

    @Override
    public Responder createResponder(int opaque) {
        return Responder.versionResponder(result, opaque);
    }

    @Override
    long cas() {
        return casToken.token();
    }

    @Override
    byte extraLength() {
        return 8;
    }

    @Override
    char keyLength() {
        return (char) key.length;
    }

    @Override
    int bodyLength() {
        return value.length;
    }

    @Override
    void writeBody(ByteBuffer buffer) {
        buffer.putInt(flags);
        buffer.putInt(expires);
        buffer.put(key);
        buffer.put(value);
    }

    @Override
    byte opcode() {
        return Opcodes.set;
    }
}
