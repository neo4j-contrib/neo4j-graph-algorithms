package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * iterate once
 * @author mknblch
 */
public interface AllRelationIterator {

    void forEachRelation(RelationConsumer consumer);

    Iterator<RelationCursor> allRelationIterator();
}
