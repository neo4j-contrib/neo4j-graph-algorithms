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
    public void testSetValue() throws Exception {
        array.set(0, 1234.5678);
        assertEquals(1234.5678, array.get(0), 0.01);
    }

    @Test
    public void testSetInfinity() throws Exception {
        array.set(0, Double.POSITIVE_INFINITY);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.01);
        array.set(0, Double.NEGATIVE_INFINITY);
        assertEquals(Double.NEGATIVE_INFINITY, array.get(0), 0.01);
    }

    @Test
    public void testAddValue() throws Exception {
        array.set(0, 123.4);
        array.add(0, 123.4);
        assertEquals(246.8, array.get(0), 0.1);
    }

    @Test
    public void testAddInf() throws Exception {
        array.set(0, Double.POSITIVE_INFINITY);
        array.add(0, 123.4);
        assertEquals(Double.POSITIVE_INFINITY, array.get(0), 0.1);
    }

    @Test
    public void testAddInfMax() throws Exception {
        array.set(0, Double.MAX_VALUE);
        array.add(0, 123.4);
        assertEquals(Double.MAX_VALUE, array.get(0), 0.1);
    }

}