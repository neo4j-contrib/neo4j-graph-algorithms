package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntPredicate;


public class PageRank {

    private final double[] pageRank, tempRank;

    private final NodeIterator nodeIterator;
    private final RelationshipIterator relationshipIterator;
    private final Degrees degrees;
    private final double dampingFactor;
    private final double alpha;
    private final int nodeCount;

    private double sum;

    public PageRank(IdMapping idMapping,
                    NodeIterator nodeIterator,
                    RelationshipIterator relationshipIterator,
                    Degrees degrees,
                    double dampingFactor) {

        this.nodeIterator = nodeIterator;
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.dampingFactor = dampingFactor;
        nodeCount = idMapping.nodeCount();
        pageRank = new double[nodeCount];
        tempRank = new double[nodeCount];
        this.alpha = 1.0 - dampingFactor;
    }

    /**
     * compute pageRank for n iterations
     */
    public PageRank compute(int iterations) {
        final Aggregator aggregator = new Aggregator();
        Arrays.fill(pageRank, 1.0 / nodeCount);
        for (int i = 0; i < iterations; i++) {
            System.arraycopy(pageRank, 0, tempRank, 0, nodeCount);
            nodeIterator.forEachNode(aggregator);
        }
        return this;
    }

    public double[] getPageRank() {
        return pageRank;
    }

    private class Aggregator implements IntPredicate, RelationshipConsumer {

        @Override
        public boolean test(final int node) {
            sum = 0;
            relationshipIterator.forEachRelationship(node, Direction.INCOMING, this);
            pageRank[node] = alpha + dampingFactor * sum;
            return true;
        }

        @Override
        public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
            sum += evaluateSubNode(targetNodeId);
            return true;
        }

        private double evaluateSubNode(int node) {
            return tempRank[node] / degrees.degree(node, Direction.OUTGOING);
        }
    }
}
