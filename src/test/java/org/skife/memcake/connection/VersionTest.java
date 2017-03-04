package org.skife.memcake.connection;

import org.junit.Test;
import org.skife.memcake.connection.Version;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class VersionTest {
    @Test
    public void testComparableSortsLowToHigh() throws Exception {
        Version one = new Version(1);
        Version two = new Version(2);

        List<Version> l = Arrays.asList(two, one);
        Collections.sort(l);

        assertThat(l).containsExactly(one, two);
    }
}
