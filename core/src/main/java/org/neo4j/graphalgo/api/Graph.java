package org.neo4j.graphalgo.api;


/**
 * Composition of often used source interfaces
 *
 * @author mknblch
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipWeights, RelationshipIterator, WeightedRelationshipIterator {

    /**
     * release resources which are not part of the result or IdMapping
     */
    default void release() {

    }
}
