package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDeque;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class ParallelTraverse {

    private final AtomicInteger threads;
    private final Graph graph;
    private final AtomicBitSet visited;
    private final ExecutorService executorService;
    private final int concurrency;
    private final ArrayList<Future<?>> futures;

    public ParallelTraverse(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        visited = new AtomicBitSet(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        threads = new AtomicInteger(0);
        futures = new ArrayList<Future<?>>();
    }

    /**
     * bfs for finding all reachable nodes of set starting from pivotNode
     */
    public void bfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer visitor) {
        if (!predicate.test(startNodeId)) {
            return;
        }
        final IntArrayDeque queue = new IntArrayDeque();
        queue.addLast(startNodeId);
        visited.set(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.removeLast();
            if (canAddThread()) {
                futures.add(executorService.submit(() -> bfs(node, direction, predicate, visitor)));
            } else {
                visitor.accept(node);
                visited.set(node);
                graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                    // should we visit this node?
                    if (!predicate.test(targetNodeId)) {
                        return true;
                    }
                    // don't visit id's twice
                    if (visited.get(targetNodeId)) {
                        return true;
                    }
                    queue.addLast(targetNodeId);
                    return true;
                });
            }
        }
    }

    public void awaitTermination() {
        ParallelUtil.awaitTermination(futures);
    }

    private boolean canAddThread() {
        final int t = threads.get();
        if (t >= concurrency) {
            return false;
        }
        return threads.compareAndSet(t, t + 1);
    }
}
