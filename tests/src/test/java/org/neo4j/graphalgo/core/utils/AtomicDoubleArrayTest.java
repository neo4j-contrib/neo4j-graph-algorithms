package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class AtomicDoubleArrayTest {

    final AtomicDoubleArray array =
            new AtomicDoubleArray(1);

    @Test
    public void test() throws Exception {
        array.set(0, 1234.5678);
        assertEquals(1234.5678, array.get(0), 0.01);
    }

    @Test
    public void test2() throws Exception {
        array.set(0, 0);
        array.addCapped(0, 123.4);
        assertEquals(123.4, array.get(0), 0.1);

    }

}