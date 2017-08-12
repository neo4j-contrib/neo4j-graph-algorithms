package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class ParallelTriangleList extends Algorithm<ParallelTriangleList> {

    private static final Direction D = Direction.BOTH;

    // gapi
    private final Graph graph;
    // executor
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;

    private final int nodeCount;

    private final int[] triangles;

    public ParallelTriangleList(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        triangles = new int[nodeCount];
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
        return null;
    }

    public ParallelTriangleList compute() {
        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int end = Math.min(i + batchSize, nodeCount);
            tasks.add(new Task(i, end));
        }
        ParallelUtil.run(tasks, executorService);
        for (Task task : tasks) {
            System.arraycopy(task.triangles, 0, this.triangles, task.startIndex, task.triangles.length);
        }
        return this;
    }



    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;
        // could be replaced with simple int counter
        private final int[] triangles;

        private Task(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            triangles = new int[endIndex - startIndex];
        }

        @Override
        public void run() {
            for (int i = startIndex; i < endIndex; i++) {
                // (u, v, w)
                final int u = i;
                graph.forEachRelationship(i, D, (_1, v, relationId) -> {
                    if (u < v) {
                        return true;
                    }
                    graph.forEachRelationship(v, D, (_2, w, relationId2) -> {
                        if (w < u) {
                            return true;
                        }
                        graph.forEachRelationship(w, D, (_3, t, relationId3) -> {
                            if (t == u) {
                                triangles[u - startIndex]++;
                            }
                            return true;
                        });
                        return true;
                    });
                    return true;
                });
            }

            //

            for (int i = 0; i < triangles.length; i++) {
                System.out.printf("[%s] %d (%d) -> %d%n",
                        Thread.currentThread().getName(),
                        i,
                        startIndex + i, triangles[i]);
            }

        }
    }
}
