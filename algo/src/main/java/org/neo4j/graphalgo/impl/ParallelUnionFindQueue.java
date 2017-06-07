package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * parallel UF implementation
 *
 * @author mknblch
 */
public class ParallelUnionFindQueue {

    private final Graph graph;
    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private final LinkedBlockingQueue<DisjointSetStruct> queue;
    private final List<Future<?>> futures;

    private DisjointSetStruct struct;

    /**
     * initialize parallel UF
     */
    public ParallelUnionFindQueue(Graph graph, ExecutorService executor, int batchSize) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
        queue = new LinkedBlockingQueue<>();
        futures = new ArrayList<>();
    }

    public ParallelUnionFindQueue compute() {

        final int steps = Math.floorDiv(nodeCount, batchSize) - 1;

        for (int i = 0; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new UnionFindTask(i)));
        }

        for (int i = steps - 1; i >= 0; i--) {
            futures.add(executor.submit(() -> {
                final DisjointSetStruct a;
                final DisjointSetStruct b;
                try {
                    a = queue.take();
                    b = queue.take();
                    queue.add(a.merge(b));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
        }

        await();

        return this;
    }

    private void await() {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
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
    }

    public ParallelUnionFindQueue compute(double threshold) {
        throw new IllegalArgumentException("Not yet implemented");
    }

    public DisjointSetStruct getStruct() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private class UnionFindTask implements Runnable {

        protected final int offset;
        protected final int end;

        public UnionFindTask(int offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        public void run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (!struct.connected(sourceNodeId, targetNodeId)) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
            queue.add(struct);
        }
    }
}
