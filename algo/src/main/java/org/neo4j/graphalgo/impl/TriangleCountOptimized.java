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

import xerial.larray.LArray;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

final public class TriangleCountOptimized {

    private LArray<long[]> adjacencyListGraph;
    private ForkJoinPool fjPool;
    private int sequentialThreshold;

    // Relationship adjacency arrays need to be sorted as required by `Arrays.binarySearch`.
    public TriangleCountOptimized(LArray<long[]> adjacencyListGraph, ForkJoinPool fjPool, int sequentialThreshold) {
        this.adjacencyListGraph = adjacencyListGraph;
        this.fjPool = fjPool;
        this.sequentialThreshold = sequentialThreshold;
    }

    public long countTriangles() {
        return fjPool.invoke(
                new CountTrianglesRecursive(
                        adjacencyListGraph, 0L, adjacencyListGraph.length(), sequentialThreshold));
    }

}

final class CountTrianglesRecursive extends RecursiveTask<Long> {

    private final LArray<long[]> adjacencyListGraph;
    private final long low;
    private final long high;
    private final int sequentialThreshold;

    public CountTrianglesRecursive(
            LArray<long[]> adjacencyListGraph,
            long low,
            long high,
            Integer sequentialThreshold) {
        this.adjacencyListGraph = adjacencyListGraph;
        this.low = low;
        this.high = high;
        this.sequentialThreshold = sequentialThreshold;
    }

    final protected long countTriangles() {
        long triangleCount = 0L;
        long firstId = low;
        while (firstId < high) {
            triangleCount += countTrianglesStartingAt(firstId);
            firstId += 1;
        }
        return triangleCount;
    }

    final protected long countTrianglesStartingAt(long firstId) {
        long count = 0;
        final long[] firstNeighbours = adjacencyListGraph.apply(firstId);
        int secondIndex = 0;
        while (secondIndex < firstNeighbours.length) {
            final long secondId = firstNeighbours[secondIndex];
            if (secondId > firstId) {
                final long[] secondNeighbours = adjacencyListGraph.apply(secondId);
                final boolean scanSecond = secondNeighbours.length < firstNeighbours.length;
                final long[] scanArray = (scanSecond) ? secondNeighbours : firstNeighbours;
                final long[] searchArray = (scanSecond) ? firstNeighbours : secondNeighbours;
                final int thirdSearchStartIndex = (scanSecond) ? secondIndex + 1 : 0;
                final int thirdScanIndex = (scanSecond) ? 0 : secondIndex + 1;
                count +=
                        countIntersection(scanArray, searchArray, thirdScanIndex, thirdSearchStartIndex, secondId);
            }
            secondIndex += 1;
        }
        return count;
    }

    final protected static long countIntersection(long[] scan, long[] search, int scanStart, int searchStart, long minId) {
        long count = 0;
        int thirdScanIndex = scanStart;
        while (thirdScanIndex < scan.length) {
            final long thirdId = scan[thirdScanIndex];
            if (thirdId > minId && Arrays.binarySearch(
                    search, searchStart, search.length, thirdId) >= 0) {
                count += 1;
            }
            thirdScanIndex += 1;
        }
        return count;
    }

    @Override
    final protected Long compute() {
        if (high - low <= sequentialThreshold) {
            return countTriangles();
        } else {
            long mid = low + (high - low) / 2;
            CountTrianglesRecursive left = new CountTrianglesRecursive(adjacencyListGraph, low, mid, sequentialThreshold);
            CountTrianglesRecursive right = new CountTrianglesRecursive(adjacencyListGraph, mid, high, sequentialThreshold);
            left.fork();
            Long rightResult = right.compute();
            Long leftResult = left.join();
            return leftResult + rightResult;
        }
    }

}
