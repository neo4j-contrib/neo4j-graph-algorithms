/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

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
public abstract class TriangleCountBase<Coeff, Self extends TriangleCountBase<Coeff, Self>> extends Algorithm<Self> {

    public static final Direction D = Direction.OUTGOING;

    private final AtomicInteger visitedNodes;
    private AtomicIntegerArray triangles;

    Graph graph;
    final int nodeCount;

    TriangleCountBase(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        triangles = new AtomicIntegerArray(nodeCount);
        visitedNodes = new AtomicInteger();
    }

    public final Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        coefficient(i)));
    }

    public abstract long getTriangleCount();

    public final AtomicIntegerArray getTriangles() {
        return triangles;
    }

    abstract double coefficient(int node);

    public abstract Coeff getClusteringCoefficients();

    abstract double getAverageClusteringCoefficient();

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    @Override
    public Self release() {
        graph = null;
        triangles = null;
        return me();
    }

    public final Self compute() {
        visitedNodes.set(0);
        runCompute();
        return me();
    }

    abstract void runCompute();

    final void exportTriangle(int u, int v, int w) {
        triangles.incrementAndGet(u);
        triangles.incrementAndGet(v);
        triangles.incrementAndGet(w);
        onTriangle();
    }

    abstract void onTriangle();

    final void nodeVisited() {
        getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
    }

    final double calculateCoefficient(int nodeId, Direction direction) {
        return calculateCoefficient(triangles.get(nodeId), graph.degree(nodeId, direction));
    }

    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return ((double) (triangles << 1)) / (degree * (degree - 1));
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
