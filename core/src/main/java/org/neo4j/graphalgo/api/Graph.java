package org.neo4j.graphalgo.api;


/**
 *
 * Composition of often used source interfaces
 *
 * @author mknblch
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipIterator, WeightedRelationshipIterator {

}
