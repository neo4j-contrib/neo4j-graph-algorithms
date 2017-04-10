package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntConsumer;


public class PageRankKernel implements IntConsumer, RelationshipConsumer {

    private final double[] pageRank;

    private final Graph graph;
    private final double dampingFactor;
    private final double alpha;

    private double sum;

    public PageRankKernel(Graph graph, double dampingFactor) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        int nodeCount = graph.nodeCount();
        pageRank = new double[nodeCount];
        this.alpha = nodeCount == 0 ? 1.0 - dampingFactor : (1.0 - dampingFactor) / nodeCount;
    }

    /**
     * compute pageRank for n iterations
     */
    public double[] compute(int iterations) {
        Arrays.fill(pageRank, 1.0);
        for (int i = 0; i < iterations; i++) {
            graph.forEachNode(this);
        }
        return pageRank;
    }

    @Override
    public void accept(final int node) {
        sum = 0;
        graph.forEachRelation(node, Direction.INCOMING, this);
        final double value = alpha + dampingFactor * sum;
        pageRank[node] = value;
    }

    @Override
    public void accept(int sourceNodeId, int targetNodeId, long relationId) {
        sum += evaluateSubNode(targetNodeId);
    }

    private double evaluateSubNode(int node) {
        int degree = graph.degree(node, Direction.OUTGOING);
        if (degree == 0) {
            return getRanking(node);
        }
        return getRanking(node) / degree;
    }

    private double getRanking(int node) {
            return pageRank[node];
        }
}
