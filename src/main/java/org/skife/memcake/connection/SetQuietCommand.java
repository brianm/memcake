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

class SetQuietCommand extends Command {
    private final CompletableFuture<Void> r;
    private final byte[] key;
    private final int flags;
    private final int expires;
    private final byte[] value;
    private final Version cas;

    SetQuietCommand(CompletableFuture<Void> r,
                    byte[] key,
                    int flags,
                    int expires,
                    byte[] value,
                    Version cas,
                    Duration timeout) {
        super(timeout);
        this.r = r;
        this.key = key;
        this.flags = flags;
        this.expires = expires;
        this.value = value;
        this.cas = cas;
    }

    @Override
    long cas() {
        return cas.token();
    }

    @Override
    Responder createResponder(int opaque) {
        return Responder.voidResponder(this, r, opaque);
    }

    @Override
    boolean isQuiet() {
        return true;
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
        return Opcodes.setq;
    }
}
