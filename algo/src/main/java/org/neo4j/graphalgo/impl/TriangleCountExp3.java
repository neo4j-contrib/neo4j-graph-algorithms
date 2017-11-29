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

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class TriangleCountExp3 extends Algorithm<TriangleCountExp3> {

    public static final Direction D = Direction.BOTH;

    private Graph graph;
    private final int nodeCount;
    private final ForkJoinPool pool;
    private final int sequentialThreshold;
    private final AtomicInteger visitedNodes;
    private final AtomicIntegerArray triangles;
    private final AtomicDoubleArray coefficients;
    private long triangleCount = -1;
    private double averageClusteringCoefficient = 0.0d;

    public TriangleCountExp3(Graph graph) {
        this(graph, ForkJoinPool.commonPool(), 10_000);
    }

    public TriangleCountExp3(Graph graph, ForkJoinPool pool, int sequentialThreshold) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.pool = pool;
        this.sequentialThreshold = sequentialThreshold;
        triangles = new AtomicIntegerArray(nodeCount);
        coefficients = new AtomicDoubleArray(nodeCount);
        visitedNodes = new AtomicInteger();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        coefficients.get(i)));
    }

    public long getTriangleCount() {
        return triangleCount;
    }

    public AtomicIntegerArray getTriangles() {
        return triangles;
    }

    @Override
    public TriangleCountExp3 me() {
        return this;
    }

    @Override
    public TriangleCountExp3 release() {
        graph = null;
        return this;
    }

    public TriangleCountExp3 compute(boolean calculateClusteringCoefficient) {
        visitedNodes.set(0);
        // calculate triangles
        triangleCount = pool.invoke(new TriangleTask(0, nodeCount));
        if (calculateClusteringCoefficient) {
            // calculate coefficients
            averageClusteringCoefficient = pool.invoke(new CoefficientTask(0, nodeCount));
        }
        return this;
    }

    public double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient / nodeCount;
    }

    public AtomicDoubleArray getClusteringCoefficients() {
        return coefficients;
    }

    private void exportTriangle(int u, int v, int w) {
        triangles.incrementAndGet(u);
        triangles.incrementAndGet(v);
        triangles.incrementAndGet(w);
    }

    private static double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return (2d * triangles) / (degree * (degree - 1));
    }

    /**
     * task for counting triangles at each node
     */
    private class TriangleTask extends RecursiveTask<Long> {

        private final int start;
        private final int end;

        private TriangleTask(int startNode, int endNode) {
            this.start = startNode;
            this.end = endNode;
        }

        @Override
        protected Long compute() {

            final int l = end - start;
            if (l > sequentialThreshold && running()) {
                final int pivot = start + l / 2;
                final TriangleTask left = new TriangleTask(start, pivot);
                final TriangleTask right = new TriangleTask(pivot, end);
                left.fork();
                return right.compute() + left.join();
            } else {
                return execute(start, end);
            }
        }

        private long execute(int startNode, int endNode) {
            final int triangles[] = {0};
            final IntStack nodes = new IntStack();
            final TerminationFlag flag = getTerminationFlag();
            final ProgressLogger progressLogger = getProgressLogger();
            final int[] head = {-1};
            for (head[0] = startNode; head[0] < endNode; head[0]++) {
                graph.forEachRelationship(head[0], D, (s, t, r) -> {
                    if (t > s) {
                        nodes.push(t);
                    }
                    return flag.running();
                });
                while (!nodes.isEmpty()) {
                    final int node = nodes.pop();
                    graph.forEachRelationship(node, D, (s, t, r) -> {
                        if (t > s && graph.exists(t, head[0], D)) {
                            exportTriangle(head[0], s, t);
                            triangles[0]++;
                        }
                        return flag.running();
                    });
                }
                progressLogger.logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
            return triangles[0];
        }

    }

    /**
     * task for calculating node clustering coefficients
     */
    private class CoefficientTask extends RecursiveTask<Double> {

        private final int start;
        private final int end;

        private CoefficientTask(int startNode, int endNode) {
            this.start = startNode;
            this.end = endNode;
        }

        @Override
        protected Double compute() {
            final int l = end - start;
            if (l > sequentialThreshold && running()) {
                final int pivot = start + l / 2;
                final CoefficientTask left = new CoefficientTask(start, pivot);
                final CoefficientTask right = new CoefficientTask(pivot, end);
                left.fork();
                return right.compute() + left.join();
            } else {
                return execute(start, end);
            }
        }

        private double execute(int start, int end) {
            double[] averageClusteringCoefficient = {0.0};
            for (int i = start; i < end; i++) {
                final double c = calculateCoefficient(triangles.get(i), graph.degree(i, TriangleCountExp3.D));
                averageClusteringCoefficient[0] += c;
                coefficients.set(i, c);
            }
            return averageClusteringCoefficient[0];
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
