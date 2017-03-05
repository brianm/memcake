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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class QuitCommand extends Command {

    private final CompletableFuture<Void> result;
    private final Connection conn;

    QuitCommand(CompletableFuture<Void> result, Connection conn, Duration timeout) {
        super(timeout);
        this.result = result;
        this.conn = conn;
    }

    @Override
    Responder createResponder(int opaque) {
        return new Responder(opaque, result::completeExceptionally, (s) -> {
            result.complete(null);
            conn.close();
        });
    }

    @Override
    byte opcode() {
        return Opcodes.quit;
    }
}
