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
 * UF implementation using an {@link AllRelationshipIterator}
 *
 * @author mknblch
 */
public class ParallelUnionFind {

    private final Graph graph;
    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private DisjointSetStruct struct;

    private final ArrayList<UFProcess> ufProcesses = new ArrayList<>();

    /**
     * initialize UF
     * @param graph
     * @param executor
     */
    public ParallelUnionFind(Graph graph, ExecutorService executor, int batchSize) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
    }

    public ParallelUnionFind compute() {
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge();
        return this;
    }

    public ParallelUnionFind compute(double threshold) {
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge();
        return this;
    }

    public DisjointSetStruct getStruct() {
        return struct;
    }

    private void merge() {
        ParallelUtil.run(ufProcesses, executor);
        final Merge merge = new Merge();
        ufProcesses.forEach(uf -> merge.structs.push(uf.struct));
        struct = ForkJoinPool.commonPool().invoke(merge); // TODO
    }

    private class UFProcess implements Runnable {

        protected final int offset;
        protected final int end;
        protected final DisjointSetStruct struct;

        public UFProcess(int offset, int length) {
            this.offset = offset;
            this.end = Math.min(offset + length, nodeCount);
            struct = new DisjointSetStruct(graph.nodeCount()).reset();
        }

        @Override
        public void run() {
            for (int node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (!struct.connected(sourceNodeId, targetNodeId)) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
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
                    if (weight < threshold) {
                        return true;
                    }
                    if (!struct.connected(sourceNodeId, targetNodeId)) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
        }
    }

    private class Merge extends RecursiveTask<DisjointSetStruct> {

        private final Stack<DisjointSetStruct> structs = new Stack<>();

        @Override
        protected DisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (size == 2) {
                return structs.pop().merge(structs.pop());
            }
            final Merge other = new Merge();
            other.add(structs.pop())
                    .add(structs.pop())
                    .fork();

            return compute().merge(other.join());
        }

        public Merge add(DisjointSetStruct struct) {
            structs.push(struct);
            return this;
        }
    }
}
