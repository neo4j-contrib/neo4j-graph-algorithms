package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.core.utils.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * @author mknobloch
 */
public class UnionFind implements RelationConsumer {

    private final Graph graph;
    private final DisjointSetStruct struct;
    private final IntStack stack;
    private final BitSet visited;

    public UnionFind(Graph graph) {
        this.graph = graph;
        struct = new DisjointSetStruct(graph.nodeCount());
        stack = new IntStack(graph.nodeCount());;
        visited = new BitSet();
    }

    public int[] compute() {

        graph.forEachNode(node -> {
            graph.forEachRelation(node, Direction.OUTGOING, this);
        });

        return struct.getSet();
    }

    @Override
    public void accept(int sourceNodeId, int targetNodeId, long relationId) {
        stack.push(sourceNodeId);
        if (!struct.isConnected(sourceNodeId, targetNodeId)) {
            struct.union(sourceNodeId, targetNodeId);
        } else {
            System.out.println("cycle");
        }
    }
}
