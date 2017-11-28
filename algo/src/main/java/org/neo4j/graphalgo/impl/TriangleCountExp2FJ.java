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
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * @author mknblch
 */
public class TriangleCountExp2FJ extends Algorithm<TriangleCountExp2FJ> {

    public static final Direction D = Direction.BOTH;
    private Graph graph;

    private final int nodeCount;
    private final AtomicInteger visitedNodes;
    private final LongAdder triangleCount;
    private AtomicIntegerArray triangles;
    private double averageClusteringCoefficient;

    private final AtomicInteger queue;

    public TriangleCountExp2FJ(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        triangles = new AtomicIntegerArray(nodeCount);
        triangleCount = new LongAdder();
        visitedNodes = new AtomicInteger();
        queue = new AtomicInteger();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        calculateCoefficient(triangles.get(i), graph.degree(i, D))));
    }

    public int getTriangleCount() {
        return triangleCount.intValue();
    }

    public AtomicIntegerArray getTriangles() {
        return triangles;
    }

    @Override
    public TriangleCountExp2FJ me() {
        return this;
    }

    @Override
    public TriangleCountExp2FJ release() {
        graph = null;
        triangles = null;
        return this;
    }

    public TriangleCountExp2FJ compute() {
        queue.set(0);
        // list of runnables
        visitedNodes.set(0);
        triangleCount.reset();
        averageClusteringCoefficient = 0.0;


        // TODO



        return this;
    }

    public double[] getClusteringCoefficients() {
        final double[] coefficient = new double[nodeCount];
        double sum = 0;
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(triangles.get(i), graph.degree(i, TriangleCountExp2FJ.D));
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
        triangleCount.increment();
        triangles.incrementAndGet(triangle[0]);
        triangles.incrementAndGet(triangle[1]);
        triangles.incrementAndGet(triangle[2]);
    }

    private void exportTriangle(int u, int v, int w) {
        triangleCount.increment();
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

    private class Task extends RecursiveAction {

        private final int startNode;
        private final int endNode;

        private Task(int startNode, int endNode) {
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        protected void compute() {
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
                        }
                        return flag.running();
                    });
                }
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
