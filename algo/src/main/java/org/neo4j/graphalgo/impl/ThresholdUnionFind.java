package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

/**
 *
 * @author mknblch
 */
public class ThresholdUnionFind {

    private final IdMapping idMapping;
    private final AllRelationshipIterator iterator;
    private final Weights weights;
    private final double threshold;

    public ThresholdUnionFind(IdMapping idMapping, AllRelationshipIterator iterator, Weights weights, double threshold) {
        this.idMapping = idMapping;
        this.iterator = iterator;
        this.weights = weights;
        this.threshold = threshold;
    }

    public DisjointSetStruct compute() {
        final ThresholdAggregator aggregator =
                new ThresholdAggregator(new DisjointSetStruct(idMapping.nodeCount()), weights, threshold);
        iterator.forEachRelationship(aggregator);
        return aggregator.struct;
    }

    private static class ThresholdAggregator implements RelationshipConsumer {

        private final DisjointSetStruct struct;
        private final double threshold;
        private final Weights weights;

        private ThresholdAggregator(DisjointSetStruct struct, Weights weights, double threshold) {
            this.struct = struct;
            this.weights = weights;
            this.threshold = threshold;
        }

        @Override
        public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
            if (weights.weightOf(sourceNodeId, targetNodeId) < threshold) {
                return true;
            }
            struct.union(sourceNodeId, targetNodeId);
            return true;
        }
    }
}
