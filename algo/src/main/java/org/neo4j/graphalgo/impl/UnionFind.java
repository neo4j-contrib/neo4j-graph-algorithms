package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.core.utils.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * work in progress
 *
 * TODO export sets
 *
 * @author mknblch
 */
public class UnionFind implements RelationConsumer {

    private final Graph graph;
    private DisjointSetStruct struct;

    public UnionFind(Graph graph) {
        this.graph = graph;
    }

    public DisjointSetStruct compute() {
        struct = new DisjointSetStruct(graph.nodeCount());
        graph.forEachNode(node -> {
            graph.forEachRelation(node, Direction.OUTGOING, this);
        });
        return struct;
    }

    // TODO visibility
    @Override
    public void accept(int sourceNodeId, int targetNodeId, long relationId) {
        if (!struct.connected(sourceNodeId, targetNodeId)) {
            struct.union(sourceNodeId, targetNodeId);
        } else {
            System.out.println("cycle");
        }
    }
}
