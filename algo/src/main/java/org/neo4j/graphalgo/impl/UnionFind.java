package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.dss.PathCompressingDSS;

/**
 *
 * @author mknblch
 */
public class UnionFind {

    private final IdMapping idMapping;
    private final AllRelationshipIterator iterator;

    public UnionFind(IdMapping idMapping, AllRelationshipIterator iterator) {
        this.idMapping = idMapping;
        this.iterator = iterator;
    }

    public DisjointSetStruct compute() {
        final Aggregator aggregator =
                new Aggregator(new PathCompressingDSS(idMapping.nodeCount()));
        iterator.forEachRelationship(aggregator);
        return aggregator.struct;
    }

    private static class Aggregator implements RelationshipConsumer {

        private final DisjointSetStruct struct;

        private Aggregator(DisjointSetStruct struct) {
            this.struct = struct;
        }

        @Override
        public void accept(int sourceNodeId, int targetNodeId, long relationId) {
            struct.union(sourceNodeId, targetNodeId);
        }
    }
}
