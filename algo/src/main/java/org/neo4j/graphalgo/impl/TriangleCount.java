package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * @author mknblch
 */
public class TriangleCount extends Algorithm<TriangleCount> {

    public static final Direction D = Direction.BOTH;

    // gapi
    private Graph graph;
    // executor
    private ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private final int nodeCount;

    private final AtomicInteger triangleCount;
    private AtomicIntegerArray triangles;

    public TriangleCount(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        triangles = new AtomicIntegerArray(nodeCount);
        triangleCount = new AtomicInteger();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), triangles.get(i)));
    }

    public int getTriangleCount() {
        return triangleCount.get();
    }

    public AtomicIntegerArray getTriangles() {
        return triangles;
    }

    @Override
    public TriangleCount me() {
        return this;
    }

    @Override
    public TriangleCount release() {
        graph = null;
        executorService = null;
        triangles = null;
        return this;
    }

    public TriangleCount compute() {
        // list of runnables
        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            // partition
            tasks.add(new Task(i, Math.min(i + batchSize, nodeCount)));
        }
        // run
        ParallelUtil.run(tasks, executorService);
        return this;
    }

    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;

        private Task(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void run() {
            final TerminationFlag flag = getTerminationFlag();
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
                                triangleCount.incrementAndGet();
                                triangles.incrementAndGet(u);
                                triangles.incrementAndGet(v);
                                triangles.incrementAndGet(w);
                                return false;
                            }
                            if (!flag.running()) {
                                return false;
                            }
                            return true;
                        });
                        return true;
                    });
                    return true;
                });
            }
        }
    }

    public static class Result {

        public final long nodeId;
        public final long triangles;

        public Result(long nodeId, long triangles) {
            this.nodeId = nodeId;
            this.triangles = triangles;
        }
    }
}
