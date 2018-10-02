package org.neo4j.graphalgo.impl.pagerank;

public interface HugeComputeStep extends Runnable {
    double[] pageRank();

    long[] starts();

    void prepareNextIteration(int[][] score);

    int[][] nextScores();

    void setStarts(long[] startArray, int[] lengthArray);
}
