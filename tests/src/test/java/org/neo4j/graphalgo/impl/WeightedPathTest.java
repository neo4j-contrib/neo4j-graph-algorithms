package org.neo4j.graphalgo.impl;

import org.junit.Test;
import org.neo4j.graphalgo.impl.yens.WeightedPath;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class WeightedPathTest {

    @Test
    public void testConcat() throws Exception {
        final WeightedPath a = new WeightedPath(3);
        final WeightedPath b = new WeightedPath(1);
        a.append(0);
        a.append(1);
        a.append(2);
        b.append(42);
        final WeightedPath concat = a.concat(b);
        assertArrayEquals(new int[]{0, 1, 2, 42}, concat.toArray());
    }

    @Test
    public void testConcatTailEmpty() throws Exception {
        final WeightedPath a = new WeightedPath(3);
        final WeightedPath b = new WeightedPath(1);
        a.append(0);
        a.append(1);
        a.append(2);
        final WeightedPath concat = a.concat(b);
        assertArrayEquals(new int[]{0, 1, 2}, concat.toArray());
    }

    @Test
    public void testConcatHeadEmpty() throws Exception {
        final WeightedPath a = new WeightedPath(3);
        final WeightedPath b = new WeightedPath(1);
        a.append(0);
        a.append(1);
        a.append(2);
        final WeightedPath concat = b.concat(a);
        assertArrayEquals(new int[]{0, 1, 2}, concat.toArray());
    }

    @Test
    public void testEquality() throws Exception {
        final WeightedPath a = new WeightedPath(3);
        final WeightedPath b = new WeightedPath(3);
        a.append(0);
        a.append(1);
        b.append(0);
        b.append(1);
        b.append(2);
        assertTrue(a.elementWiseEquals(b, 2));
        assertTrue(a.elementWiseEquals(b, 1));
        assertFalse(a.elementWiseEquals(b, 3));
        assertTrue(b.elementWiseEquals(a, 2));
        assertTrue(b.elementWiseEquals(a, 1));
        assertFalse(b.elementWiseEquals(a, 3));
    }

    @Test
    public void testGrow() throws Exception {
        final WeightedPath p = new WeightedPath(0);
        p.append(0);
        p.append(1);
        assertEquals(2, p.size());
    }

}
