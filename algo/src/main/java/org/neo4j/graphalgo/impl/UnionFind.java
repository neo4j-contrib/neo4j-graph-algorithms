package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.Objects;

/**
 * Sequential UnionFind:
 *
 * The algorithm computes sets of weakly connected components.
 *
 * This impl. is using the AllRelationshipIterator which is able
 * to iterate over each relationships without the need to supply a start
 * node
 *
 * @author mknblch
 */
public class UnionFind {

    private final AllRelationshipIterator iterator;
    private final int nodeCount;

    /**
     * initialize UF
     *
     * @param idMapping the id mapping
     * @param iterator an AllRelationshipIterator
     */
    public UnionFind(IdMapping idMapping, AllRelationshipIterator iterator) {
        this.iterator = iterator;
        this.nodeCount = idMapping.nodeCount();
    }

    /**
     * compute unions using an additional constraint
     * @param constraint the join constraint
     * @return a DSS
     */
    public DisjointSetStruct compute(IntBinaryPredicate constraint) {
        Objects.requireNonNull(constraint);
        final DisjointSetStruct dss = new DisjointSetStruct(nodeCount);
        dss.reset();
        iterator.forEachRelationship((source, target, relationship) -> {
            if (constraint.test(source, target)) {
                dss.union(source, target);
            }
            return true;
        });
        return dss;
    }

    /**
     * compute unions of connected nodes
     * @return a DSS
     */
    public DisjointSetStruct compute() {
        final DisjointSetStruct dss = new DisjointSetStruct(nodeCount);
        dss.reset();
        iterator.forEachRelationship((source, target, relationship) -> {
            dss.union(source, target);
            return true;
        });
        return dss;
    }
}
