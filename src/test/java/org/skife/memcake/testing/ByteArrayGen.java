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
