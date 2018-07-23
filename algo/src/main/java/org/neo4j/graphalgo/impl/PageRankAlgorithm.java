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
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface PageRankAlgorithm {

    PageRankAlgorithm compute(int iterations);

    PageRankResult result();

    Algorithm<?> algorithm();

    static PageRankAlgorithm of(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return of(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph);
    }

    static PageRankAlgorithm of(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph) {
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(tracker, huge, dampingFactor, sourceNodeIds);
        }
        return new PageRank(graph, dampingFactor, sourceNodeIds);
    }

    static PageRankAlgorithm of(
        Graph graph,
        double dampingFactor,
        LongStream sourceNodeIds,
        ExecutorService pool,
        int concurrency,
        int batchSize) {
        return of(AllocationTracker.EMPTY, graph, dampingFactor, sourceNodeIds, pool, concurrency, batchSize);
    }

    static PageRankAlgorithm of(
            AllocationTracker tracker,
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(
                    pool,
                    concurrency,
                    batchSize,
                    tracker,
                    huge,
                    dampingFactor,
                    sourceNodeIds
                    );
        }
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                graph,
                dampingFactor,
                sourceNodeIds);
    }
}
