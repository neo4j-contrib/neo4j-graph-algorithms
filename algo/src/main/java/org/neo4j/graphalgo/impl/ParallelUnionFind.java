package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    private DisjointSetStruct struct;

    /**
     * initialize UF
     * @param graph
     * @param executor
     */
    public ParallelUnionFind(Graph graph, ExecutorService executor) {
        this.graph = graph;
        this.executor = executor;
        nodeCount = graph.nodeCount();
    }

    public ParallelUnionFind compute(int batchSize) {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();

        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }

        ParallelUtil.run(ufProcesses, executor);

        final ArrayList<DisjointSetStruct> temp = new ArrayList<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct));

        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));

        return this;
    }

    public DisjointSetStruct getStruct() {
        return struct;
    }

    private class UFProcess implements Runnable {

        private final int offset;
        private final int end;
        private final DisjointSetStruct struct;

        public UFProcess(int offset, int length) {
            this.offset = offset;
            this.end = offset + length;

            struct = new DisjointSetStruct(graph.nodeCount()).reset();
        }

        @Override
        public void run() {
            for (int node = offset; node < end && node < nodeCount; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    struct.union(sourceNodeId, targetNodeId);
                    return true;
                });
            }

        }

    }

    private class Merge extends RecursiveTask<DisjointSetStruct> {

        private final List<DisjointSetStruct> structs;

        private Merge(List<DisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected DisjointSetStruct compute() {

            final int size = structs.size();

            if (size == 1) {
                return structs.get(0);
            }
            if (size == 2) {
                return merge(structs.remove(1), structs.remove(0));
            }
            final ArrayList<DisjointSetStruct> list = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                list.add(structs.remove(i));
            }

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
