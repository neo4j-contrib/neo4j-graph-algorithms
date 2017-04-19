package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class GraphUnionFind {

    private final Graph graph;

    public GraphUnionFind(Graph graph) {
        this.graph = graph;
    }

    public DisjointSetStruct compute() {
        final Aggregator aggregator =
                new Aggregator(new DisjointSetStruct(graph.nodeCount()));

        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, aggregator);
        });

        return aggregator.struct;
    }

    private static class Aggregator implements RelationshipConsumer {

        private final DisjointSetStruct struct;

        private Aggregator(DisjointSetStruct struct) {
            this.struct = struct;
        }

        @Override
        public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
            struct.union(sourceNodeId, targetNodeId);
            return true;
        }
    }
}
