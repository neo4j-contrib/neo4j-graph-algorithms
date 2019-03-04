package org.neo4j.graphalgo.similarity;

import org.junit.Test;

import static org.junit.Assert.*;

public class CategoricalInputTest {

    @Test
    public void overlapShowsSmallerSideFirst() {
        CategoricalInput one = new CategoricalInput(3, new long[]{1, 2, 3, 4});
        CategoricalInput two = new CategoricalInput(7, new long[]{1, 2, 3});

        SimilarityResult result = one.overlap(0.0, two);

        assertEquals(7, result.item1);
        assertEquals(3, result.item2);
        assertEquals(3, result.count1);
        assertEquals(4, result.count2);
        assertEquals(3, result.intersection);
        assertEquals(1.0, result.similarity, 0.01);
    }

    @Test
    public void overlapShouldNotInferReverseIfRequestedNotTo() {
        CategoricalInput one = new CategoricalInput(3, new long[]{1, 2, 3, 4});
        CategoricalInput two = new CategoricalInput(7, new long[]{1, 2, 3});

        SimilarityResult result = one.overlap(0.0, two, false);
        assertNull(result);
    }
}