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
package org.skife.memcake.testing;

import com.google.auto.service.AutoService;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

@AutoService(Generator.class)
public class ByteArrayGen extends Generator<byte[]> {
    private int min = 1;
    private int max = Character.MAX_VALUE;

    public ByteArrayGen() {
        super(byte[].class);
    }

    @Override
    public byte[] generate(SourceOfRandomness random, GenerationStatus status) {
        int len = random.nextInt(min, max);
        byte[] rs = new byte[len];
        for (int i = 0; i < len; i++) {
            rs[i] = random.nextByte(Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return rs;
    }

    public void configure(Size size) {
        this.min = size.min();
        this.max = size.max();
    }
}
