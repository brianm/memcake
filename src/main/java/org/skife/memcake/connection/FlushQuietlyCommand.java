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

class FlushQuietlyCommand extends FlushCommand {
    FlushQuietlyCommand(CompletableFuture<Void> result,
                        int expires,
                        Duration timeout) {
        super(result, expires, timeout);
    }

    @Override
    byte opcode() {
        return Opcodes.flushq;
    }

    @Override
    boolean isQuiet() {
        return true;
    }
}
