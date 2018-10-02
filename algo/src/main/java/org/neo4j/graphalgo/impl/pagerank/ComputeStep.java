package org.neo4j.graphalgo.impl.pagerank;

public interface ComputeStep extends Runnable {
    int[][] nextScores();

    double[] pageRank();

    int[] starts();

    void setStarts(int[] startArray, int[] lengthArray);

    void prepareNextIteration(int[][] score);
}
