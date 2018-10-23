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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public interface PageRankAlgorithm {

    PageRankAlgorithm compute(int iterations);

    PageRankResult result();

    Algorithm<?> algorithm();

    static PageRankAlgorithm weightedOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds
            ) {
        return weightedOf(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph, false);
    }

    static PageRankAlgorithm weightedOf(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph,
            boolean cacheWeights) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(tracker, huge, dampingFactor, sourceNodeIds, pageRankVariant);
        }

        return new PageRank(graph, dampingFactor, sourceNodeIds, pageRankVariant);
    }

    static PageRankAlgorithm articleRankOf(
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds) {
        return articleRankOf(AllocationTracker.EMPTY, dampingFactor, sourceNodeIds, graph);
    }

    static PageRankAlgorithm articleRankOf(
            AllocationTracker tracker,
            double dampingFactor,
            LongStream sourceNodeIds,
            Graph graph) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(tracker, huge, dampingFactor, sourceNodeIds, pageRankVariant);
        }

        return new PageRank(graph, dampingFactor, sourceNodeIds, pageRankVariant);
    }

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
        PageRankVariant computeStepFactory = new NonWeightedPageRankVariant();

        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(tracker, huge, dampingFactor, sourceNodeIds, computeStepFactory);
        }

        return new PageRank(graph, dampingFactor, sourceNodeIds, computeStepFactory);
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
        PageRankVariant pageRankVariant = new NonWeightedPageRankVariant();
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(
                    pool,
                    concurrency,
                    batchSize,
                    tracker,
                    huge,
                    dampingFactor,
                    sourceNodeIds,
                    pageRankVariant
                    );
        }

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant);
    }

    static PageRankAlgorithm weightedOf(
            AllocationTracker tracker,
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            boolean cacheWeights) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(
                    pool,
                    concurrency,
                    batchSize,
                    tracker,
                    huge,
                    dampingFactor,
                    sourceNodeIds,
                    pageRankVariant
            );
        }

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant);
    }

    static PageRankAlgorithm articleRankOf(
            AllocationTracker tracker,
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(
                    pool,
                    concurrency,
                    batchSize,
                    tracker,
                    huge,
                    dampingFactor,
                    sourceNodeIds,
                    pageRankVariant
            );
        }

        return new PageRank(
                pool,
                concurrency,
                batchSize,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant);
    }
}
