package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.dss.PathCompressingDSS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author mknblch
 */
public class ThresholdUnionFind {

    private final IdMapping idMapping;
    private final WeightedAllRelationshipIterator iterator;
    private final double threshold;

    public ThresholdUnionFind(IdMapping idMapping, WeightedAllRelationshipIterator iterator, double threshold) {
        this.idMapping = idMapping;
        this.iterator = iterator;
        this.threshold = threshold;
    }

    public DisjointSetStruct compute() {
        final ThresholdAggregator aggregator =
                new ThresholdAggregator(new PathCompressingDSS(idMapping.nodeCount()), threshold);
        iterator.forEachRelationship(aggregator);
        return aggregator.struct;
    }

    private static class ThresholdAggregator implements WeightedRelationshipConsumer {

        private final DisjointSetStruct struct;
        private final double threshold;

        private ThresholdAggregator(DisjointSetStruct struct, double threshold) {
            this.struct = struct;
            this.threshold = threshold;
        }

        @Override
        public void accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
            if (weight < threshold) {
                return;
            }
            struct.union(sourceNodeId, targetNodeId);
        }
    }
}
