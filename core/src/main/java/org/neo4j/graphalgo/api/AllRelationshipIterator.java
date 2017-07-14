package org.neo4j.graphalgo.api;

/**
 * The AllRelationshipIterator is intended to iterate over each relationship
 * once or until its consumer decides to stop the iteration.
 *
 * @author mknblch
 */
public interface AllRelationshipIterator {

    /**
     * called once for each relation
     */
    void forEachRelationship(RelationshipConsumer consumer);
}
