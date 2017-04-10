package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterates over each relation
 *
 * @author mknblch
 */
public interface AllRelationIterator {

    /**
     * called once for each relation
     */
    void forEachRelation(RelationConsumer consumer);

    /**
     * get an iterator for each relation
     */
    Iterator<RelationCursor> allRelationIterator();
}
