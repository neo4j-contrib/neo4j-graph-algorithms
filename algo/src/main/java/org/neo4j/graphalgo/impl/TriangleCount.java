/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
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

    private Graph graph;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private final AtomicInteger visitedNodes;
    private final AtomicInteger triangleCount;
    private AtomicIntegerArray triangles;
    private double averageClusteringCoefficient;

    public TriangleCount(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        triangles = new AtomicIntegerArray(nodeCount);
        triangleCount = new AtomicInteger();
        visitedNodes = new AtomicInteger();
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
        visitedNodes.set(0);
        triangleCount.set(0);
        averageClusteringCoefficient = 0.0;
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


    public double[] getClusteringCoefficients() {
        final double[] coefficient = new double[nodeCount];
        double sum = 0;
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(triangles.get(i), graph.degree(i, TriangleCount.D));
            coefficient[i] = c;
            sum += c;
        }
        averageClusteringCoefficient = sum / nodeCount;
        return coefficient;
    }

    public double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient;
    }

    private void exportTriangle(int u, int v, int w) {
        triangleCount.incrementAndGet();
        triangles.incrementAndGet(u);
        triangles.incrementAndGet(v);
        triangles.incrementAndGet(w);
    }

    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return (2d * triangles) / (degree * (degree - 1));
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
                                exportTriangle(u, v, w);
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
