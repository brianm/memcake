package org.skife.memcake;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BitsTest {

    @Test
    public void testBigInts() throws Exception {
        int val = Integer.MAX_VALUE + 2;
        byte[] bytes = Bits.intToBytes(val);

        long uns = Bits.toUnsignedInt(bytes);
        assertThat(uns).isEqualTo(val);
        assertThat(Bits.intToBytes(Integer.MAX_VALUE + 1)).isEqualTo(new byte[] {-128, 0, 0, 0});
    }

    @Test
    public void testSimpler() throws Exception {
        long v = Bits.toUnsignedInt(new byte[] {0x00, 0x00, 0x00, 0x01});
        assertThat(v).isEqualTo(1);
    }
}
