package org.neo4j.graphalgo.core.utils.traverse;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDeque;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class SequentialTraverse {

    private final Graph graph;
    private final SimpleBitSet visited;
    private final IntDeque queue;

    public SequentialTraverse(Graph graph) {
        this.graph = graph;
        visited = new SimpleBitSet(graph.nodeCount());
        queue = new IntArrayDeque();
    }

    /**
     * bfs for finding all reachable nodes of set starting from pivotNode
     */
    public void bfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer consumer) {
        if (!predicate.test(startNodeId)) {
            return;
        }
        reset();
        queue.addLast(startNodeId);
        visited.put(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.removeLast();
            consumer.accept(node);
            visited.put(node);
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // should we visit this node?
                if (!predicate.test(targetNodeId)) {
                    return true;
                }
                // don't visit id's twice
                if (visited.contains(targetNodeId)) {
                    return true;
                }
                queue.addLast(targetNodeId);
                return true;
            });
        }
    }

    /**
     * dfs for finding all reachable nodes of set starting from pivotNode
     */
    public void dfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer consumer) {
        if (!predicate.test(startNodeId)) {
            return;
        }
        reset();
        queue.addLast(startNodeId);
        visited.put(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.removeFirst();
            consumer.accept(node);
            visited.put(node);
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // should we visit this node?
                if (!predicate.test(targetNodeId)) {
                    return true;
                }
                // don't visit id's twice
                if (visited.contains(targetNodeId)) {
                    return true;
                }
                queue.addLast(targetNodeId);
                return true;
            });
        }
    }

    private void reset() {
        queue.clear();
        visited.clear();
    }
}
