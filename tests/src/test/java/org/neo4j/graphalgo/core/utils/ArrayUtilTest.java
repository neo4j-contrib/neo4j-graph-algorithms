package org.neo4j.graphalgo.core.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ArrayUtilTest {

    final private int[] testData;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Integer> data() {
        return Arrays.asList(
                32, 33,
                64, 65,
                2048, 2049,
                4096, 4097
        );
    }

    public ArrayUtilTest(int size) {
        testData = new int[size];
        Arrays.setAll(testData, i -> (i + 1) * 2);
    }

    @Test
    public void testBinarySearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i * 2) + 1));
        }
    }

    @Test
    public void testLinearSearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i * 2) + 1));
        }
    }
}