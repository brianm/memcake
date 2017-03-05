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

public class Version implements Comparable<Version> {
    public static final Version NONE = new Version(0);
    private final long version;

    Version(long version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version1 = (Version) o;

        return version == version1.version;
    }

    @Override
    public int hashCode() {
        return (int) (version ^ (version >>> 32));
    }


    public long token() {
        return version;
    }

    @Override
    public int compareTo(Version o) {
        return (int) (this.token() - o.token());
    }
}
