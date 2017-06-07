package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 *
 * @author mknblch
 */
public class ParallelUnionFindFJMerge {

    private final Graph graph;
    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private DisjointSetStruct struct;

    /**
     * initialize UF
     * @param graph
     * @param executor
     */
    public ParallelUnionFindFJMerge(Graph graph, ExecutorService executor, int batchSize) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
    }

    public ParallelUnionFindFJMerge compute() {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return this;
    }

    public ParallelUnionFindFJMerge compute(double threshold) {

        final ArrayList<TUFProcess> ufProcesses = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return this;
    }

    public void merge(ArrayList<? extends UFProcess> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        final Stack<DisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct));
        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public DisjointSetStruct getStruct() {
        return struct;
    }

    private class UFProcess implements Runnable {

        protected final int offset;
        protected final int end;
        protected final DisjointSetStruct struct;

        public UFProcess(int offset, int length) {
            this.offset = offset;
            this.end = offset + length;
            struct = new DisjointSetStruct(graph.nodeCount()).reset();
        }

        @Override
        public void run() {
            for (int node = offset; node < end && node < nodeCount; node++) {
                try {

                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        if (!struct.connected(sourceNodeId, targetNodeId)) {
                            struct.union(sourceNodeId, targetNodeId);
                        }
                        return true;
                    });
                } catch (Exception e) {
                    System.out.println("exception for nodeid:" + node);
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    private class TUFProcess extends UFProcess {

        private final double threshold;

        public TUFProcess(int offset, int length, double threshold) {
            super(offset, length);
            this.threshold = threshold;
        }

        @Override
        public void run() {
            for (int node = offset; node < end && node < nodeCount; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                    if (weight > threshold) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
        }
    }

    private class Merge extends RecursiveTask<DisjointSetStruct> {

        private final Stack<DisjointSetStruct> structs;

        private Merge(Stack<DisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected DisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (size == 2) {
                return merge(structs.pop(), structs.pop());
            }
            final Stack<DisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final DisjointSetStruct computed = mergeB.compute();
            return merge(mergeA.join(), computed);
        }

        private DisjointSetStruct merge(DisjointSetStruct a, DisjointSetStruct b) {
            return a.merge(b);
        }
    }



}
