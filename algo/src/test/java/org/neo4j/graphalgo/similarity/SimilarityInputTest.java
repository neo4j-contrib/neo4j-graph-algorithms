package org.neo4j.graphalgo.similarity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.similarity.SimilarityInput.extractInputIds;

public class SimilarityInputTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void findOneItem() {
        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids), Arrays.asList( 5L));

        assertArrayEquals(indexes, new int[] {0});
    }

    @Test
    public void findMultipleItems() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids), Arrays.asList( 5L, 9L));

        assertArrayEquals(indexes, new int[] {0, 4});
    }

    @Test
    public void missingItem() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Node ids [10] do not exist in node ids list");

        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids), Arrays.asList( 10L));

        assertArrayEquals(indexes, new int[] {});
    }

    @Test
    public void allMissing() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Node ids [10, 11, -1, 29] do not exist in node ids list");

        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids), Arrays.asList( 10L ,11L, -1L, 29L));

        assertArrayEquals(indexes, new int[] {});
    }

    @Test
    public void someMissingSomeFound() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Node ids [10, 12] do not exist in node ids list");

        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids), Arrays.asList( 10L ,5L, 7L, 12L));

        assertArrayEquals(indexes, new int[] {0, 2});
    }

}