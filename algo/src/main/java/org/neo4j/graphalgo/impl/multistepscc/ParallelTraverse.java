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
 * parallel breadth first search
 *
 * @author mknblch
 */
public class ParallelTraverse {

    // the graph
    private final Graph graph;
    // number of active threads
    private final AtomicInteger threads;
    // set of visited ID's
    private final AtomicBitSet visited;
    // the executor
    private final ExecutorService executorService;
    // intended number of concurrently active threads
    private final int concurrency;
    // result future queue
    private final ConcurrentLinkedQueue<Future<?>> futures;

    public ParallelTraverse(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        visited = new AtomicBitSet(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        threads = new AtomicInteger(0);
        futures = new ConcurrentLinkedQueue<>();
    }

    /**
     * reset underlying container
     * @return itself
     */
    public ParallelTraverse reset() {
        visited.clear();
        futures.clear();
        return this;
    }

    /**
     * wait for all started futures to complete
     * @return itself
     */
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
     * start bfs at startNodeId using the supplied direction. On each relationship the targetNode is tested
     * using the predicate. If it succeeds the node is enqueued for the next iteration. Upon first arrival at a
     * node the visitor is called with its node Id.
     *
     * NOTE: predicate and visitor must be thread safe
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

    /**
     * tell whether a new thread can be added or not
     * @return true if there is room for another thread, false otherwise
     */
    private boolean canAddThread() {
        final int t = threads.get();
        if (t >= concurrency - 1) {
            return false;
        }
        return threads.compareAndSet(t, t + 1);
    }
}
