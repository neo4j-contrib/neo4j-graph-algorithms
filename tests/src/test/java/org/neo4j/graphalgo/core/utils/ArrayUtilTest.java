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

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{32, "32"},
                new Object[]{33, "33"},
                new Object[]{64, "64"},
                new Object[]{65, "65"},
                new Object[]{128, "128"},
                new Object[]{129, "129"},
                new Object[]{2048, "2048"},
                new Object[]{2049, "2049"}
        );
    }

    public ArrayUtilTest(int size, String ignored) {
        testData = new int[size];
        Arrays.setAll(testData, i -> (i + 1) * 2);
    }

    @Test
    public void testBinarySearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i + 1) * 2));
        }
        for (int i = 0; i < testData.length; i++) {
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i * 2) + 1));

        }
    }

    @Test
    public void testLinearSearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i + 1) * 2));
        }
        for (int i = 0; i < testData.length; i++) {
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i * 2) + 1));

        }
    }
}