package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * parallel UnionFind using ExecutorService and common ForkJoin-Pool.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * Like in {@link ParallelUnionFindForkJoin} the resulting DSS of each node-partition
 * is merged by the ForkJoin pool while calculating the DSS is done by the
 * ExecutorService.
 * <p>
 * This might lead to a better distribution of tasks in the merge-tree.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindFJMerge extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, HugeParallelUnionFindFJMerge> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private HugeDisjointSetStruct struct;

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    HugeParallelUnionFindFJMerge(
            HugeGraph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph);
        this.executor = executor;
        this.tracker = tracker;
        nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);
    }

    public HugeDisjointSetStruct compute() {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return getStruct();
    }

    public HugeDisjointSetStruct compute(double threshold) {
        final Collection<TUFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return getStruct();
    }

    private void merge(Collection<? extends UFTask> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<HugeDisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct()));
        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public HugeDisjointSetStruct getStruct() {
        return struct;
    }

    @Override
    public HugeParallelUnionFindFJMerge release() {
        struct = null;
        return super.release();
    }

    private abstract class UFTask implements Runnable {
        abstract HugeDisjointSetStruct struct();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class UFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final HugeDisjointSetStruct struct;
        private final HugeRelationshipIterator rels;

        UFProcess(long offset, long length) {
            this.offset = offset;
            this.end = offset + length;
            struct = new HugeDisjointSetStruct(nodeCount, tracker).reset();
            rels = graph.concurrentCopy();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                try {
                    rels.forEachRelationship(
                            node,
                            Direction.OUTGOING,
                            (sourceNodeId, targetNodeId) -> {
                                struct.union(sourceNodeId, targetNodeId);
                                return true;
                            });
                } catch (Exception e) {
                    System.out.println("exception for nodeid:" + node);
                    e.printStackTrace();
                    return;
                }
            }
            getProgressLogger().logProgress((end - 1) / (nodeCount - 1));
        }

        @Override
        HugeDisjointSetStruct struct() {
            return struct;
        }
    }

    /**
     * Process to calc a DSS using a threshold
     */
    private class TUFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final HugeDisjointSetStruct struct;
        private final HugeRelationshipIterator rels;
        private final double threshold;

        TUFProcess(long offset, long length, double threshold) {
            this.offset = offset;
            this.end = offset + length;
            this.threshold = threshold;
            struct = new HugeDisjointSetStruct(nodeCount, tracker).reset();
            rels = graph.concurrentCopy();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            double weight = graph.weightOf(
                                    sourceNodeId,
                                    targetNodeId);
                            if (weight > threshold) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
        }

        @Override
        HugeDisjointSetStruct struct() {
            return struct;
        }
    }

    private class Merge extends RecursiveTask<HugeDisjointSetStruct> {

        private final Stack<HugeDisjointSetStruct> structs;

        private Merge(Stack<HugeDisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected HugeDisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (!running()) {
                return structs.pop();
            }
            if (size == 2) {
                return structs.pop().merge(structs.pop());
            }
            final Stack<HugeDisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final HugeDisjointSetStruct computed = mergeB.compute();
            return mergeA.join().merge(computed);
        }
    }


}
