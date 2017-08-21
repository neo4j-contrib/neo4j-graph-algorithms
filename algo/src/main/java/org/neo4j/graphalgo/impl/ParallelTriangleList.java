package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ParallelTriangleList extends Algorithm<ParallelTriangleList> {

    public static final Direction D = Direction.BOTH;

    // gapi
    private final Graph graph;
    // executor
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private final int nodeCount;

    private final AtomicIntegerArray triangles;


    public ParallelTriangleList(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        triangles = new AtomicIntegerArray(nodeCount);
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), triangles.get(i)));
    }

    @Override
    public ParallelTriangleList me() {
        return this;
    }

    @Override
    public ParallelTriangleList release() {
        return this;
    }

    public ParallelTriangleList compute() {

        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int end = Math.min(i + batchSize, nodeCount);
            tasks.add(new Task(i, end));
        }
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
            for (int i = startIndex; i < endIndex; i++) {
                // (u, v, w)
                graph.forEachRelationship(i, D, (u, v, relationId) -> {
                    if (u >= v) {
                        return true;
                    }
                    graph.forEachRelationship(v, D, (v2, w, relationId2) -> {
                        if (v2 >= w) {
                            return true;
                        }
                        graph.forEachRelationship(w, D, (sourceNodeId3, t, relationId3) -> {
                            if (t == u) {
                                triangles.incrementAndGet(u);
                                triangles.incrementAndGet(v);
                                triangles.incrementAndGet(w);
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
