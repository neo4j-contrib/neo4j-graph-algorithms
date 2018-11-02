package org.neo4j.graphalgo.similarity;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class RleDecoderTest {
    @Test
    public void readTwoArrays() {
        RleDecoder rleDecoder = new RleDecoder(2);

        double[] item1 = Weights.buildRleWeights(Arrays.asList(4.0, 4.0), 1);
        double[] item2 = Weights.buildRleWeights(Arrays.asList(3.0, 3.0), 1);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);
    }

    @Test
    public void readSameArraysAgain() {
        RleDecoder rleDecoder = new RleDecoder(2);

        double[] item1 = Weights.buildRleWeights(Arrays.asList(4.0, 4.0), 1);
        double[] item2 = Weights.buildRleWeights(Arrays.asList(3.0, 3.0), 1);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);
    }
}