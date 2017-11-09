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
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;

/**
 * parallel UnionFind using common ForkJoin-Pool only.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The UnionFindTask extracts a nodePartition if its input-set is too big and
 * calculates its result while lending the rest-nodeSet to another FJ-Task.
 * <p>
 * Note: The splitting method might be sub-optimal since the resulting work-tree is
 * very unbalanced so each thread needs to wait for its predecessor to complete
 * before merging his set into the parent set.
 *
 * @author mknblch
 */
public class ParallelUnionFindForkJoin extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ParallelUnionFindForkJoin> {

    private final int nodeCount;
    private final int batchSize;

    public static Function<Graph, ParallelUnionFindForkJoin> of(int minBatchSize, int concurrency) {
        return graph -> new ParallelUnionFindForkJoin(
                graph,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize parallel UF
     *
     * @param graph
     */
    public ParallelUnionFindForkJoin(
            Graph graph,
            int minBatchSize,
            int concurrency) {
        super(graph);
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);

    }

    public DisjointSetStruct compute() {
        return ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
    }


    public DisjointSetStruct compute(double threshold) {
        return ForkJoinPool
                .commonPool()
                .invoke(new ThresholdUFTask(0, threshold));
    }

    private class UnionFindTask extends RecursiveTask<DisjointSetStruct> {

        protected final int offset;
        protected final int end;

        UnionFindTask(int offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        protected DisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId, relationId) -> {
                            if (!struct.connected(sourceNodeId, targetNodeId)) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
            getProgressLogger().logProgress(end - 1, nodeCount - 1);

            return struct;
        }
    }

    private class ThresholdUFTask extends UnionFindTask {

        private final double threshold;

        ThresholdUFTask(int offset, double threshold) {
            super(offset);
            this.threshold = threshold;
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId, relationId, weight) -> {
                            if (weight < threshold) {
                                return true;
                            }
                            if (!struct.connected(sourceNodeId, targetNodeId)) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
            return struct;
        }
    }
}
