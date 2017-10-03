package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.Objects;

/**
 * Sequential UnionFind:
 * <p>
 * The algorithm computes sets of weakly connected components.
 * <p>
 * This impl. is using the AllRelationshipIterator which is able
 * to iterate over each relationships without the need to supply a start
 * node
 *
 * @author mknblch
 */
public class UnionFind extends Algorithm<UnionFind> {

    private AllRelationshipIterator iterator;
    private final int nodeCount;
    private int stepCount;

    /**
     * initialize UF
     *
     * @param idMapping the id mapping
     * @param iterator  an AllRelationshipIterator
     */
    public UnionFind(IdMapping idMapping, AllRelationshipIterator iterator) {
        this.iterator = iterator;
        this.nodeCount = Math.toIntExact(idMapping.nodeCount());
    }

    /**
     * compute unions using an additional constraint
     *
     * @param constraint the join constraint
     * @return a DSS
     */
    public DisjointSetStruct compute(IntBinaryPredicate constraint) {
        Objects.requireNonNull(constraint);
        final ProgressLogger progressLogger = getProgressLogger();
        final DisjointSetStruct dss = new DisjointSetStruct(nodeCount);
        dss.reset();
        stepCount = 0;
        iterator.forEachRelationship((source, target, relationship) -> {
            if (constraint.test(source, target)) {
                dss.union(source, target);
            }
            stepCount++;
            progressLogger.logProgress((double) stepCount / (nodeCount - 1));
            return running();
        });
        return dss;
    }

    /**
     * compute unions of connected nodes
     *
     * @return a DSS
     */
    public DisjointSetStruct compute() {
        final DisjointSetStruct dss = new DisjointSetStruct(nodeCount);
        final ProgressLogger progressLogger = getProgressLogger();
        dss.reset();
        stepCount = 0;
        iterator.forEachRelationship((source, target, relationship) -> {
            dss.union(source, target);
            stepCount++;
            progressLogger.logProgress((double) stepCount / (nodeCount - 1));
            return running();
        });
        return dss;
    }

    @Override
    public UnionFind me() {
        return this;
    }

    @Override
    public UnionFind release() {
        iterator = null;
        return this;
    }
}
