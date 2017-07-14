package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * parallel UnionFind using common ForkJoin-Pool only.
 *
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 *
 * The UnionFindTask extracts a nodePartition if its input-set is too big and
 * calculates its result while lending the rest-nodeSet to another FJ-Task.
 *
 * Note: The splitting method might be sub-optimal since the resulting work-tree is
 * very unbalanced so each thread needs to wait for its predecessor to complete
 * before merging his set into the parent set.
 *
 * @author mknblch
 */
public class ParallelUnionFindForkJoin extends Algorithm<ParallelUnionFindForkJoin> {

    private final Graph graph;
    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private DisjointSetStruct struct;

    /**
     * initialize parallel UF
     * @param graph
     * @param executor
     */
    public ParallelUnionFindForkJoin(Graph graph, ExecutorService executor, int minBatchSize, int concurrency) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustBatchSize(graph.nodeCount(), concurrency, minBatchSize);

    }

    public ParallelUnionFindForkJoin compute() {

        struct = ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
        return this;
    }


    public ParallelUnionFindForkJoin compute(double threshold) {
        struct = ForkJoinPool.commonPool().invoke(new ThresholdUFTask(0, threshold));
        return this;
    }

    public DisjointSetStruct getStruct() {
        return struct;
    }

    @Override
    public ParallelUnionFindForkJoin me() {
        return this;
    }

    private class UnionFindTask extends RecursiveTask<DisjointSetStruct> {

        protected final int offset;
        protected final int end;

        public UnionFindTask(int offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        protected DisjointSetStruct compute() {
            if (nodeCount - end >= batchSize) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (!struct.connected(sourceNodeId, targetNodeId)) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
            getProgressLogger().logProgress((end - 1.0) / (nodeCount - 1.0));
            return struct;
        }
    }

    private class ThresholdUFTask extends UnionFindTask {

        private final double threshold;

        public ThresholdUFTask(int offset, double threshold) {
            super(offset);
            this.threshold = threshold;
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
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
