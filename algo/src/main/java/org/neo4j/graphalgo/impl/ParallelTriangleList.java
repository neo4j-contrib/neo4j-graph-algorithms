package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
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

    private int[] triangles;
    private int triangleCount;

    public ParallelTriangleList(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), triangles[i]));
    }

    public int getTriangleCount() {
        return triangleCount;
    }

    public int[] getTriangles() {
        return triangles;
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
        // list of runnables
        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            // partition
            final int end = Math.min(i + batchSize, nodeCount);
            tasks.add(new Task(i, end));
        }
        // run
        ParallelUtil.run(tasks, executorService);
        // merge
        triangles = new int[nodeCount];
        triangleCount = 0;
        for (Task task : tasks) {
            System.arraycopy(task.triangles, 0, this.triangles, task.startIndex, task.triangles.length);
            triangleCount += task.triangleCount;
        }
        return this;
    }

    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;
        private final int[] triangles;
        private int triangleCount = 0;

        private Task(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.triangles = new int[endIndex - startIndex];
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
                                triangleCount++;
                                triangles[u]++;
                                triangles[v]++;
                                triangles[w]++;
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
