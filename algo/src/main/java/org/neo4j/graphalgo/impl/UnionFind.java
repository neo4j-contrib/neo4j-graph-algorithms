package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.api.WeightedRelationConsumer;
import org.neo4j.graphalgo.core.utils.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.PathCompressingDSS;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * work in progress
 *
 * TODO export sets
 *
 * @author mknblch
 */
public class UnionFind {

    private final Graph graph;

    public UnionFind(Graph graph) {
        this.graph = graph;
    }

    public Map<Integer, List<Integer>> compute() {
        final Aggregator aggregator =
                new Aggregator(new PathCompressingDSS(graph.nodeCount()));

        graph.forEachNode(node -> {
            graph.forEachRelation(node, Direction.OUTGOING, aggregator);
        });

        return export(aggregator.struct);
    }

    public Map<Integer, List<Integer>> compute(double threshold) {
        final ThresholdAggregator aggregator =
                new ThresholdAggregator(new PathCompressingDSS(graph.nodeCount()), threshold);

        graph.forEachNode(node -> {
            graph.forEachRelation(node, Direction.OUTGOING, aggregator);
        });

        return export(aggregator.struct);
    }

    // TODO find a better container
    private Map<Integer, List<Integer>> export(DisjointSetStruct dss) {
        final HashMap<Integer, List<Integer>> map = new HashMap<>();
        for (int i = dss.count() - 1; i >= 0; i--) {
            int k = dss.find(i);
            if (map.containsKey(k)) {
                map.get(k).add(i);
            } else {
                ArrayList<Integer> list = new ArrayList<>();
                list.add(i);
                map.put(k, list);
            }
        }
        return map;
    }

    private static class Aggregator implements RelationConsumer {

        private final DisjointSetStruct struct;

        private Aggregator(DisjointSetStruct struct) {
            this.struct = struct;
        }

        @Override
        public void accept(int sourceNodeId, int targetNodeId, long relationId) {
            struct.union(sourceNodeId, targetNodeId);
        }
    }

    private static class ThresholdAggregator implements WeightedRelationConsumer {

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
