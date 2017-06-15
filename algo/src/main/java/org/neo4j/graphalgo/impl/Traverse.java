package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class Traverse {

    private final Graph graph;

    public Traverse(Graph graph) {
        this.graph = graph;
    }

    /**
     * bfs for finding all reachable nodes of set starting from pivotNode
     */
    public void bfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer consumer) {
        System.out.println("BFS on startNodeId = " + startNodeId);
        if (!predicate.test(startNodeId)) {
            return;
        }
        final SimpleBitSet visited = new SimpleBitSet(graph.nodeCount());
        final IntStack queue = new IntStack();
        queue.push(startNodeId);
        visited.put(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            consumer.accept(node);
            System.out.println(node);
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // should we visit this node?
                if (!predicate.test(targetNodeId)) {
                    return true;
                }
                // don't visit id's twice
                if (visited.contains(targetNodeId)) {
                    return true;
                }
                queue.push(targetNodeId);
                return true;
            });
        }
    }
}
