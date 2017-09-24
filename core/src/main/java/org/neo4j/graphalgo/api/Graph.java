package org.neo4j.graphalgo.api;


/**
 *
 * Composition of often used source interfaces
 *
 * @author mknblch
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipIterator, WeightedRelationshipIterator {

    /**
     * release resources which are not part of the result or IdMapping
     */
    default void release() {

    }

    /**
     * Returns a view of this graph that is only guaranteed to be safe when accessed by at most one concurrent thread.
     */
    default Graph singleThreaded() {
        return this;
    }

}
