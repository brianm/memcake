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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Optional;

public class Value {
    private final Version cas;
    private final int flags;
    private final Optional<byte[]> key;
    private final byte[] value;

    Value(Version cas, int flags, Optional<byte[]> key, byte[] value) {
        this.cas = cas;
        this.flags = flags;
        this.key = key;
        this.value = value;
    }

    public Version getVersion() {
        return cas;
    }

    public int getFlags() {
        return flags;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "naming is awkward, but is is fine")
    public byte[] getValue() {
        return value;
    }

    public Optional<byte[]> getKey() {
        return key;
    }
}
