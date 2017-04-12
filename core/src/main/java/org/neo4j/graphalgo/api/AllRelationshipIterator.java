package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterates over each relation
 *
 * @author mknblch
 */
public interface AllRelationshipIterator {

    /**
     * called once for each relation
     */
    void forEachRelationship(RelationshipConsumer consumer);

    /**
     * get an iterator for each relation
     */
    Iterator<RelationshipCursor> allRelationshipIterator();
}
