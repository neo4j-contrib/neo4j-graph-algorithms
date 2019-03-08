package org.neo4j.graphalgo.impl.results;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CentralityResultTest {
    @Test
    public void doubleArrayResult() {
        DoubleArrayResult result = new DoubleArrayResult(new double[] {1,2,3,4});

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }

    @Test
    public void partitionedPrimitiveDoubleArrayResult() {
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        int[] starts = new int[] { 0, 2};
        PartitionedPrimitiveDoubleArrayResult result = new PartitionedPrimitiveDoubleArrayResult(partitions, starts);

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }

    @Test
    public void partitionedDoubleArrayResult() {
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        long[] starts = new long[] { 0, 2};
        PartitionedDoubleArrayResult result = new PartitionedDoubleArrayResult(partitions, starts);

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }
}