package org.neo4j.graphalgo.api;


/**
 * @author mknblch
 *         added 02.03.2017.
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipIterator, WeightedRelationshipIterator {

}
