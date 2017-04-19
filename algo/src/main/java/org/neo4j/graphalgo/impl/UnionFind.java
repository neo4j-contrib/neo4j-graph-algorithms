package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

/**
 * UF implementation using an {@link AllRelationshipIterator}
 *
 * @author mknblch
 */
public class UnionFind {

    private final AllRelationshipIterator iterator;

    private DisjointSetStruct dss;
    private final IntBinaryPredicate constraint;

    /**
     * initialize UF without additional constraint
     *
     * @param idMapping the id mapping
     * @param iterator the AllRelationshipIterator
     */
    public UnionFind(IdMapping idMapping, AllRelationshipIterator iterator) {
        this(idMapping, iterator, null);
    }

    /**
     * initialize UF using an additional constraint before merging
     * @param idMapping the id mapping
     * @param iterator the AllRelationshipIterator
     * @param constraint the constraint
     */
    public UnionFind(IdMapping idMapping, AllRelationshipIterator iterator, IntBinaryPredicate constraint) {
        this.iterator = iterator;
        this.constraint = constraint != null ? constraint : (p, q) -> true;
        this.dss = new DisjointSetStruct(idMapping.nodeCount());
    }

    public DisjointSetStruct compute() {
        dss.reset();
        iterator.forEachRelationship((source, target, relationship) -> {
            if (constraint.test(source, target)) {
                dss.union(source, target);
            }
            return true;
        });
        return dss;
    }
}
