package org.neo4j.graphalgo.impl.louvain;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class Communities {

    private final double[] weightSum;

    private final int[][] community;
    private final double[][] sTot;
    private final int capacity;

    private int flip = 0;

    public Communities(int capacity) {
        this.capacity = capacity;
        community = new int[2][this.capacity];
        sTot = new double[2][capacity];
        weightSum = new double[capacity];
    }

    public void reset(int index, double sum) {
        sTot[0][index] = weightSum[index] = sum;
        community[0][index] = index;
    }

    public void flip() {
        flip++;
//        System.arraycopy(weightSum, 0, sTot[(flip + 1) % 2], 0, capacity);
        Arrays.fill(sTot[(flip + 1) % 2], 0.0);
    }

    public int get(int index) {
        return community[flip % 2][index];
    }

    public double getSTot(int index) {
        return sTot[flip % 2][index];
    }

    public void move(int index, int targetCommunity) {
        final int idx = (flip + 1) % 2;
        final int sourceCommunity  = community[flip % 2][index];
        community[idx][index] = targetCommunity;
        sTot[idx][sourceCommunity] -= weightSum[index];
        sTot[idx][targetCommunity] += weightSum[index];
    }

    public void print() {
        final StringBuilder builder = new StringBuilder();

        builder.append(String.format("Flip %d%n", flip % 2));
        builder.append("I: \t");
        for (int i = 0; i < capacity; i++) {
            builder.append(String.format(" %2d |", i));
        }
        builder.append("\nw: \t");
        for (int i = 0; i < capacity; i++) {
            builder.append(String.format(" %2.0f |", weightSum[i]));
        }
        builder.append("\ntR:\t");
        for (int i = 0; i < capacity; i++) {
            builder.append(String.format(" %2.0f |", sTot[flip % 2][i]));
        }
        builder.append("\ntW:\t");
        for (int i = 0; i < capacity; i++) {
            builder.append(String.format(" %2.0f |", sTot[(flip + 1) % 2][i]));
        }

        System.out.println(builder.toString());


    }

}
