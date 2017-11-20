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

import com.carrotsearch.hppc.LongArrayList;
import xerial.larray.LArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

final public class Triangles {

    private LArray<long[]> adjacencyListGraph;
    private ForkJoinPool fjPool;
    private int sequentialThreshold;

    // Relationship adjacency arrays need to be sorted as required by `Arrays.binarySearch`.
    public Triangles(LArray<long[]> adjacencyListGraph, ForkJoinPool fjPool, int sequentialThreshold) {
        this.adjacencyListGraph = adjacencyListGraph;
        this.fjPool = fjPool;
        this.sequentialThreshold = sequentialThreshold;
    }

    public long count() {
        return fjPool.invoke(
                new CountTrianglesRecursive(
                        adjacencyListGraph, 0L, adjacencyListGraph.length(), sequentialThreshold));
    }

    public TriangleIterator find() {
        TriangleIterator i = new TriangleIterator();
        fjPool.execute(
                new FindTrianglesRecursive(
                        true, adjacencyListGraph, 0L, adjacencyListGraph.length(), sequentialThreshold, i));
        return i;
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
                final boolean scanSecond = secondNeighbours.length < firstNeighbours.length - (secondIndex + 1);
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
        int i = scanStart;
        while (i < scan.length) {
            final long candidateId = scan[i];
            if (candidateId > minId && Arrays.binarySearch(
                    search, searchStart, search.length, candidateId) >= 0) {
                count += 1;
            }
            i += 1;
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

final class FindTrianglesRecursive extends RecursiveTask<Void> {

    private final boolean isRoot;
    private final LArray<long[]> adjacencyListGraph;
    private final long low;
    private final long high;
    private final int sequentialThreshold;
    private final TriangleIterator resultIterator;

    public FindTrianglesRecursive(
            boolean isRoot,
            LArray<long[]> adjacencyListGraph,
            long low,
            long high,
            Integer sequentialThreshold,
            TriangleIterator resultIterator) {
        this.isRoot = isRoot;
        this.adjacencyListGraph = adjacencyListGraph;
        this.low = low;
        this.high = high;
        this.sequentialThreshold = sequentialThreshold;
        this.resultIterator = resultIterator;
    }

    final protected void findTriangles() {
        long firstId = low;
        while (firstId < high) {
            findTrianglesStartingAt(firstId);
            firstId += 1;
        }
    }

    final protected void findTrianglesStartingAt(long firstId) {
        ArrayList<TriangleStream.Result> results = new ArrayList<>();
        final long[] firstNeighbours = adjacencyListGraph.apply(firstId);
        int secondIndex = 0;
        while (secondIndex < firstNeighbours.length) {
            final long secondId = firstNeighbours[secondIndex];
            if (secondId > firstId) {
                final long[] secondNeighbours = adjacencyListGraph.apply(secondId);
                final boolean scanSecond = secondNeighbours.length < firstNeighbours.length - (secondIndex + 1);
                final long[] scanArray = (scanSecond) ? secondNeighbours : firstNeighbours;
                final long[] searchArray = (scanSecond) ? firstNeighbours : secondNeighbours;
                final int thirdSearchStartIndex = (scanSecond) ? secondIndex + 1 : 0;
                final int thirdScanIndex = (scanSecond) ? 0 : secondIndex + 1;
                LongArrayList thirdIds = findIntersection(
                        scanArray, searchArray, thirdScanIndex, thirdSearchStartIndex, secondId);
                int length = thirdIds.size();
                int i = 0;
                while (i < length) {
                    results.add(new TriangleStream.Result(firstId, secondId, thirdIds.get(i)));
                    i += 1;
                }
            }
            secondIndex += 1;
        }
        if (results.size() > 0) {
            resultIterator.add(results);
        }
    }

    final protected static LongArrayList findIntersection(long[] scan, long[] search, int scanStart, int searchStart, long minId) {
        LongArrayList intersectingIds = new LongArrayList();
        int i = scanStart;
        while (i < scan.length) {
            final long candidateId = scan[i];
            if (candidateId > minId && Arrays.binarySearch(
                    search, searchStart, search.length, candidateId) >= 0) {
                intersectingIds.add(candidateId);
            }
            i += 1;
        }
        return intersectingIds;
    }

    @Override
    final protected Void compute() {
        if (high - low <= sequentialThreshold) {
            findTriangles();
        } else {
            long mid = low + (high - low) / 2;
            FindTrianglesRecursive left = new FindTrianglesRecursive(
                    false, adjacencyListGraph, low, mid, sequentialThreshold, resultIterator);
            FindTrianglesRecursive right = new FindTrianglesRecursive(
                    false, adjacencyListGraph, mid, high, sequentialThreshold, resultIterator);
            left.fork();
            right.compute();
            left.join();
        }
        if (isRoot) {
            // Add empty collection to indicate that the computation has ended.
            resultIterator.add(new ArrayList<>());
        }
        return null;
    }

}
