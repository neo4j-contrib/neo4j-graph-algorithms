package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class ParallelTriangleCount extends Algorithm<ParallelTriangleCount> {

    public static final Direction D = Direction.BOTH;

    // gapi
    private final Graph graph;
    // executor
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private final int nodeCount;
    private long triangleCount;


    public ParallelTriangleCount(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
    }

    public long getTriangleCount() {
        return triangleCount;
    }

    @Override
    public ParallelTriangleCount me() {
        return this;
    }

    @Override
    public ParallelTriangleCount release() {
        return this;
    }

    public ParallelTriangleCount compute() {

        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int end = Math.min(i + batchSize, nodeCount);
            tasks.add(new Task(i, end));
        }
        ParallelUtil.run(tasks, executorService);

        triangleCount = tasks.stream()
                .mapToLong(Task::count)
                .sum();

        return this;
    }

    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;
        private int count = 0;

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
                                count++;
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

        private int count() {
            return count;
        }
    }
}
