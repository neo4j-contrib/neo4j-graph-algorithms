package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntConsumer;


public class PageRankAlgo implements IntConsumer, RelationshipConsumer {

    private final double[] pageRank;

    private final NodeIterator nodeIterator;
    private final RelationshipIterator relationshipIterator;
    private final Degrees degrees;
    private final double dampingFactor;
    private final double alpha;

    private double sum;

    public PageRankAlgo(IdMapping idMapping,
                        NodeIterator nodeIterator,
                        RelationshipIterator relationshipIterator,
                        Degrees degrees,
                        double dampingFactor) {

        this.nodeIterator = nodeIterator;
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.dampingFactor = dampingFactor;
        int nodeCount = idMapping.nodeCount();
        pageRank = new double[nodeCount];
        this.alpha = nodeCount == 0
                ? 1.0 - dampingFactor
                : (1.0 - dampingFactor) / nodeCount;
    }

    /**
     * compute pageRank for n iterations
     */
    public double[] compute(int iterations) {
        Arrays.fill(pageRank, 1.0);
        for (int i = 0; i < iterations; i++) {
            nodeIterator.forEachNode(this);
        }
        return pageRank;
    }

    @Override
    public void accept(final int node) {
        sum = 0;
        relationshipIterator.forEachRelationship(node, Direction.INCOMING, this);
        pageRank[node] = alpha + dampingFactor * sum;
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        sum += evaluateSubNode(targetNodeId);
        return true;
    }

    private double evaluateSubNode(int node) {
        final int degree = degrees.degree(node, Direction.OUTGOING);
        if (degree == 0) {
            return getRanking(node);
        }
        return getRanking(node) / degree;
    }

    private double getRanking(int node) {
            return pageRank[node];
        }
}
