package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * @author mknblch
 */
public class TriangleStream extends Algorithm<TriangleStream> {

    public static final Direction D = Direction.BOTH;
    private Graph graph;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private AtomicInteger visitedNodes;
    private AtomicInteger runningThreads;
    private BlockingQueue<Result> resultQueue;

    public TriangleStream(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        this.resultQueue = new LinkedBlockingQueue<>();
        runningThreads = new AtomicInteger();
        visitedNodes = new AtomicInteger();
    }

    @Override
    public TriangleStream me() {
        return this;
    }

    @Override
    public TriangleStream release() {
        visitedNodes = null;
        runningThreads = null;
        resultQueue = null;
        graph = null;
        executorService = null;
        return this;
    }

    public Stream<Result> resultStream() {
        submitTasks();
        final TerminationFlag flag = getTerminationFlag();
        final Iterator<Result> it = new Iterator<Result>() {

            @Override
            public boolean hasNext() {
                return flag.running() && (runningThreads.get() > 0 || !resultQueue.isEmpty());
            }

            @Override
            public Result next() {
                Result result = null;
                try {
                    while (hasNext() && result == null) {
                        result = resultQueue.poll(1, TimeUnit.SECONDS);
                    }
                    return result;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(it, 0), false)
                .filter(Objects::nonNull);
    }

    private void submitTasks() {
        runningThreads.set(0);
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            // partition
            final int end = Math.min(i + batchSize, nodeCount);
            executorService.execute(new Task(i, end));
        }
    }

    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;

        private Task(int startIndex, int endIndex) {
            runningThreads.incrementAndGet();
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void run() {
            final TerminationFlag flag = getTerminationFlag();
            final ProgressLogger progressLogger = getProgressLogger();
            for (int i = startIndex; i < endIndex; i++) {
                // (u, v, w)
                graph.forEachRelationship(i, D, (u, v, relationId) -> {
                    if (u >= v) {
                        return true;
                    }
                    if (!flag.running()) {
                        return false;
                    }
                    graph.forEachRelationship(v, D, (v2, w, relationId2) -> {
                        if (v2 >= w) {
                            return true;
                        }
                        if (!flag.running()) {
                            return false;
                        }
                        graph.forEachRelationship(w, D, (sourceNodeId3, t, relationId3) -> {
                            if (t == u) {
                                try {
                                    resultQueue.put(new Result(
                                            graph.toOriginalNodeId(u),
                                            graph.toOriginalNodeId(v),
                                            graph.toOriginalNodeId(w)));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return false;
                            }
                            return flag.running();
                        });
                        return true;
                    });
                    return true;
                });
                progressLogger.logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
            runningThreads.decrementAndGet();
        }
    }

    public static class Result {

        public final long nodeA;
        public final long nodeB;
        public final long nodeC;

        public Result(long nodeA, long nodeB, long nodeC) {
            this.nodeA = nodeA;
            this.nodeB = nodeB;
            this.nodeC = nodeC;
        }

        @Override
        public String toString() {
            return "Triangle{" +
                    nodeA +
                    ", " + nodeB +
                    ", " + nodeC +
                    '}';
        }
    }
}
