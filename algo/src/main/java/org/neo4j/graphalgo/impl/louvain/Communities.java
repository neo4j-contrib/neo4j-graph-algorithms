package org.neo4j.graphalgo.impl.louvain;

/**
 * @author mknblch
 */
public class Communities {

    private final double[] weightSum;

    private final int[][] community;
    private final double[][] sTot;

    private int flip = 0;

    public Communities(int capacity) {
        community = new int[2][capacity];
        sTot = new double[2][capacity];
        weightSum = new double[capacity];
    }

    public void reset(int index, double sum) {
        this.weightSum[index] = sum;
        sTot[0][index] = sum;
        community[0][index] = index;
    }

    public void flip() {
        flip++;
    }

    public double get(int index) {
        return community[flip % 2][index];
    }

    public void move(int node, int sourceCommunity, int targetCommunity) {
        final int idx = (flip + 1) % 2;
        community[idx][node] = targetCommunity;
        sTot[idx][sourceCommunity] -= weightSum[node];
        sTot[idx][targetCommunity] += weightSum[node];
    }

}
