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

class AddCommand extends SetCommand {
    AddCommand(CompletableFuture<Version> result,
               byte[] key,
               int flags,
               int expires,
               byte[] value,
               Duration timeout) {
        super(result, key, flags, expires, value, Version.NONE, timeout);
    }

    @Override
    protected byte opcode() {
        return Opcodes.add;
    }
}
