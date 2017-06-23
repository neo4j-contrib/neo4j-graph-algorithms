package org.neo4j.graphalgo.impl.multistepscc;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphalgo.core.utils.queue.IntMaxPriorityQueue;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.concurrent.*;
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
    private final ConcurrentLinkedQueue<Future<?>> futures;

    public ParallelTraverse(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        visited = new AtomicBitSet(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        threads = new AtomicInteger(0);
        futures = new ConcurrentLinkedQueue<>();
    }

    public ParallelTraverse reset() {
        visited.clear();
        futures.clear();
        return this;
    }

    public ParallelTraverse awaitTermination() {

        boolean done = false;
        Throwable error = null;
        try {

            while (!futures.isEmpty()) {
                try {
                    futures.poll().get();
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
        return this;
    }

    /**
     * bfs for finding reachable nodes
     */
    public ParallelTraverse bfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer visitor) {
        if (!predicate.test(startNodeId)) {
            return this;
        }
        final IntMaxPriorityQueue queue = new IntMaxPriorityQueue();
        queue.add(startNodeId, 0d);
        visited.set(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            if (canAddThread()) {
                futures.add(executorService.submit(() -> bfs(node, direction, predicate, visitor)));
            } else {
                if (visited.trySet(node)) {
                    visitor.accept(node);
                    graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                        // should we visit this node?
                        if (!predicate.test(targetNodeId)) {
                            return true;
                        }
                        // don't visit id's twice
                        if (visited.get(targetNodeId)) {
                            return true;
                        }
                        queue.add(targetNodeId, graph.degree(targetNodeId, direction));
                        return true;
                    });
                }
            }
        }
        threads.decrementAndGet();
        return this;
    }

    private boolean canAddThread() {
        final int t = threads.get();
        if (t >= concurrency - 1) {
            return false;
        }
        return threads.compareAndSet(t, t + 1);
    }
}
