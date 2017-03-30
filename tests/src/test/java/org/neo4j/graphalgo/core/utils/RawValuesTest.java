package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 *         added 30.03.2017.
 */
public class RawValuesTest {

    @Test
    public void testPositive() throws Exception {
        long combinedValue = RawValues.combineIntInt(42, 1337);
        assertEquals(42, RawValues.getHead(combinedValue));
        assertEquals(1337, RawValues.getTail(combinedValue));
    }

    @Test
    public void testNegative() throws Exception {
        long combinedValue = RawValues.combineIntInt(-1337, -42);
        assertEquals(-1337, RawValues.getHead(combinedValue));
        assertEquals(-42, RawValues.getTail(combinedValue));
    }

    @Test
    public void testMixed() throws Exception {
        long combinedValue = RawValues.combineIntInt(-42, 1337);
        assertEquals(-42, RawValues.getHead(combinedValue));
        assertEquals(1337, RawValues.getTail(combinedValue));

        combinedValue = RawValues.combineIntInt(42, -1337);
        assertEquals(42, RawValues.getHead(combinedValue));
        assertEquals(-1337, RawValues.getTail(combinedValue));

    }
}
