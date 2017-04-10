package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterates over each relation
 *
 * @author mknblch
 */
public interface AllRelationIterator {

    /**
     *
     */
    void forEachRelation(RelationConsumer consumer);

    Iterator<RelationCursor> allRelationIterator();
}
