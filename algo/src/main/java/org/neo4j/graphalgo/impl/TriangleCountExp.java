package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.container.FlipStack;
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
public class TriangleCountExp extends Algorithm<TriangleCountExp> {

    public static final Direction D = Direction.BOTH;
    private Graph graph;

    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private final AtomicInteger visitedNodes;
    private final AtomicInteger triangleCount;
    private AtomicIntegerArray triangles;
    private double averageClusteringCoefficient;

    private final AtomicInteger blockQueue;

    public TriangleCountExp(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        triangles = new AtomicIntegerArray(nodeCount);
        triangleCount = new AtomicInteger();
        visitedNodes = new AtomicInteger();
        blockQueue = new AtomicInteger();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        calculateCoefficient(triangles.get(i), graph.degree(i, D))));
    }

    public int getTriangleCount() {
        return triangleCount.get();
    }

    public AtomicIntegerArray getTriangles() {
        return triangles;
    }

    @Override
    public TriangleCountExp me() {
        return this;
    }

    @Override
    public TriangleCountExp release() {
        graph = null;
        executorService = null;
        triangles = null;
        return this;
    }

    public TriangleCountExp compute() {
        blockQueue.set(0);
        // list of runnables
        visitedNodes.set(0);
        triangleCount.set(0);
        averageClusteringCoefficient = 0.0;
        final ArrayList<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            // create tasks
            tasks.add(new Task());
        }
        // run
        ParallelUtil.run(tasks, executorService);
        return this;
    }


    public double[] getClusteringCoefficients() {
        final double[] coefficient = new double[graph.nodeCount()];
        double sum = 0;
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(triangles.get(i), graph.degree(i, TriangleCountExp.D));
            coefficient[i] = c;
            sum += c;
        }
        averageClusteringCoefficient = sum / nodeCount;
        return coefficient;
    }

    public double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient;
    }

    private void exportTriangle(int[] triangle) {
        triangleCount.incrementAndGet();
        triangles.incrementAndGet(triangle[0]);
        triangles.incrementAndGet(triangle[1]);
        triangles.incrementAndGet(triangle[2]);
    }

    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return (2d * triangles) / (degree * (degree - 1));
    }

    private class Task implements Runnable {

        private final IntStack level0, level1, level2;

        private Task() {
            level0 = new IntStack();
            level1 = new IntStack();
            level2 = new IntStack();
        }

//        @Override
//        public void run() {
//            final TerminationFlag flag = getTerminationFlag();
//            final ProgressLogger progressLogger = getProgressLogger();
//            final int[] triangle = {-1, -1, -1}; // (u, v, w)
//            // process triangles (u, v, w) at node u
//            while ((triangle[0] = blockQueue.getAndIncrement()) < nodeCount) {
//                // fill level0 stack with v's
//                graph.forEachRelationship(triangle[0], D, (sourceNodeId, targetNodeId, relationId) -> {
//                    if (sourceNodeId >= targetNodeId) {
//                        return true;
//                    }
//                    if (!flag.running()) {
//                        return false;
//                    }
//                    level0.push(targetNodeId);
//                    return true;
//                });
//                // Level1 - for each v of (u, v, w)
//                while (!level0.isEmpty()) {
//                    triangle[1] = level0.pop();
//
//                    graph.forEachRelationship(triangle[1], D, (sourceNodeId, targetNodeId, relationId2) -> {
//                        if (sourceNodeId >= targetNodeId) {
//                            return true;
//                        }
//                        if (!flag.running()) {
//                            return false;
//                        }
//                        level1.push(targetNodeId);
//                        return true;
//                    });
//
//                    while (!level1.isEmpty()) {
//                        triangle[2] = level1.pop();
//                        if (!flag.running()) {
//                            return false;
//                        }
//                        if (triangle[2] == triangle[0]) {
//                            exportTriangle(triangle);
//                        }
//                    }
//                }
//                // Level w
//
//                progressLogger.logProgress(triangle[0], nodeCount - 1);
//            }
//        }

        @Override
        public void run() {
            final TerminationFlag flag = getTerminationFlag();
            final ProgressLogger progressLogger = getProgressLogger();
            final int[] triangle = {-1, -1, -1}; // (u, v, w)
            while ((triangle[0] = blockQueue.getAndIncrement()) < nodeCount) {
                // (u, v, w)
                graph.forEachRelationship(triangle[0], D, (u, v, relationId) -> {
                    if (u >= v) {
                        return true;
                    }
                    if (!flag.running()) {
                        return false;
                    }
                    triangle[1] = v;
                    graph.forEachRelationship(triangle[1], D, (v2, w, relationId2) -> {
                        if (v2 >= w) {
                            return true;
                        }
                        if (!flag.running()) {
                            return false;
                        }
                        triangle[2] = w;
                        graph.forEachRelationship(w, D, (sourceNodeId3, t, relationId3) -> {
                            if (t == u) {
                                exportTriangle(triangle);
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
        }
    }

    public static class Result {

        public final long nodeId;
        public final long triangles;
        public final double coefficient;

        public Result(long nodeId, long triangles, double coefficient) {
            this.nodeId = nodeId;
            this.triangles = triangles;
            this.coefficient = coefficient;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", triangles=" + triangles +
                    ", coefficient=" + coefficient +
                    '}';
        }
    }
}
